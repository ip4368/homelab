//! The CP replica's Raft state on disk: a snapshot of the whole
//! [`Durable`] plus an append-only log of the [`DurableOp`]s since, one JSON
//! object per line, each synced before the replica sends anything that
//! depends on it. Loading replays the ops onto the snapshot.
//!
//! Each snapshot has a generation and its own ops file (`ops-<n>.jsonl`), so
//! compacting is one atomic step (renaming the new snapshot into place):
//! power lost before it keeps the old pair, after it the new one, and ops
//! already folded into a snapshot are never replayed onto it again. A
//! snapshot without a generation (an older build's) pairs with `ops.jsonl`.

use control::proto::LogCmd;
use control::raft::{Durable, DurableOp};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

pub struct RaftStore {
    dir: PathBuf,
    /// The stored snapshot's generation, once read or written.
    generation: Option<u64>,
    ops: Option<File>,
}

#[derive(Serialize)]
struct StoredRef<'a> {
    generation: u64,
    state: &'a Durable<LogCmd>,
}

#[derive(Deserialize)]
struct Stored {
    generation: u64,
    state: Durable<LogCmd>,
}

impl RaftStore {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into(), generation: None, ops: None }
    }

    fn snapshot_path(&self) -> PathBuf {
        self.dir.join("snapshot.json")
    }

    fn ops_path(&self, generation: u64) -> PathBuf {
        match generation {
            0 => self.dir.join("ops.jsonl"),
            n => self.dir.join(format!("ops-{n}.jsonl")),
        }
    }

    /// The stored snapshot and its generation, if there is one.
    fn read_snapshot(&self) -> io::Result<Option<(u64, Durable<LogCmd>)>> {
        let snap = match fs::read(self.snapshot_path()) {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        if let Ok(s) = serde_json::from_slice::<Stored>(&snap) {
            return Ok(Some((s.generation, s.state)));
        }
        let legacy: Durable<LogCmd> = serde_json::from_slice(&snap).map_err(io::Error::other)?;
        Ok(Some((0, legacy)))
    }

    /// The stored state, if any: the snapshot with every op since applied.
    /// A torn last line (power lost mid-write) ends the replay: its op was
    /// never synced, so nothing depended on it.
    pub fn load(&self) -> io::Result<Option<Durable<LogCmd>>> {
        let Some((generation, mut d)) = self.read_snapshot()? else { return Ok(None) };
        let ops = match fs::read(self.ops_path(generation)) {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::NotFound => vec![],
            Err(e) => return Err(e),
        };
        for line in ops.split(|&b| b == b'\n').filter(|l| !l.is_empty()) {
            let Ok(op) = serde_json::from_slice::<DurableOp<LogCmd>>(line) else { break };
            d.apply(op);
        }
        Ok(Some(d))
    }

    /// Folds the ops into a new snapshot, so the next load replays nothing.
    pub fn compact(&mut self) -> io::Result<()> {
        match self.load()? {
            Some(d) => self.store(&d),
            None => Ok(()),
        }
    }

    /// Replaces whatever is stored with `d`: an empty ops file for the next
    /// generation, then the snapshot naming it (the atomic step), then the
    /// old ops files go.
    pub fn store(&mut self, d: &Durable<LogCmd>) -> io::Result<()> {
        fs::create_dir_all(&self.dir)?;
        self.ops = None;
        let current = match self.generation {
            Some(g) => g,
            None => self.read_snapshot()?.map_or(0, |(g, _)| g),
        };
        let next = current + 1;
        File::create(self.ops_path(next))?.sync_all()?;
        sync_dir(&self.dir)?;
        let snapshot = serde_json::to_vec(&StoredRef { generation: next, state: d }).map_err(io::Error::other)?;
        write_synced(&self.snapshot_path(), &snapshot)?;
        self.generation = Some(next);
        for old in (0..next).map(|g| self.ops_path(g)).filter(|p| p.exists()) {
            let _ = fs::remove_file(old);
        }
        sync_dir(&self.dir)
    }

    /// Appends `op` and syncs it. A no-op if nothing was ever stored.
    pub fn persist(&mut self, op: &DurableOp<LogCmd>) -> io::Result<()> {
        if self.ops.is_none() {
            let generation = match self.generation {
                Some(g) => g,
                None => match self.read_snapshot()? {
                    Some((g, _)) => g,
                    None => return Ok(()),
                },
            };
            self.generation = Some(generation);
            self.ops = Some(OpenOptions::new().create(true).append(true).open(self.ops_path(generation))?);
        }
        let f = self.ops.as_mut().expect("opened above");
        let mut line = serde_json::to_vec(op).map_err(io::Error::other)?;
        line.push(b'\n');
        f.write_all(&line)?;
        f.sync_data()
    }
}

/// Writes `path` atomically: a synced temporary file renamed over it.
pub fn write_synced(path: &Path, data: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    let mut f = File::create(&tmp)?;
    f.write_all(data)?;
    f.sync_all()?;
    fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent() {
        sync_dir(dir)?;
    }
    Ok(())
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use control::raft::{Entry, EntryPayload};
    use model::HostId;

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("homelab-store-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    fn noop(term: u64, index: u64) -> Entry<LogCmd> {
        Entry { term, index, payload: EntryPayload::Noop }
    }

    fn ops_files(dir: &Path) -> Vec<PathBuf> {
        let mut v: Vec<PathBuf> = fs::read_dir(dir).unwrap().flatten().map(|e| e.path()).filter(|p| p.extension().is_some_and(|x| x == "jsonl")).collect();
        v.sort();
        v
    }

    #[test]
    fn ops_survive_a_restart_and_a_torn_tail_is_ignored() {
        let dir = tmp("restart");
        let mut s = RaftStore::new(&dir);
        assert_eq!(s.load().unwrap(), None);
        s.persist(&DurableOp::SetHardState { term: 1, voted_for: None }).unwrap();
        assert_eq!(s.load().unwrap(), None, "nothing is persisted before a store");

        let start = Durable::new([HostId::from("a")].into());
        s.store(&start).unwrap();
        let ops = [
            DurableOp::SetHardState { term: 2, voted_for: Some(HostId::from("a")) },
            DurableOp::Append(vec![noop(2, 1), noop(2, 2), noop(2, 3)]),
            DurableOp::TruncateFrom(3),
        ];
        let mut want = start.clone();
        for op in &ops {
            s.persist(op).unwrap();
            want.apply(op.clone());
        }
        // Power lost mid-line, inside a multi-byte character.
        let mut f = OpenOptions::new().append(true).open(dir.join("ops-1.jsonl")).unwrap();
        f.write_all(b"{\"SetHardState\":{\"term\":3,\"voted_for\":\"\xc3").unwrap();

        let mut again = RaftStore::new(&dir);
        let got = again.load().unwrap().unwrap();
        assert_eq!(got, want);
        assert_eq!((got.term, got.log.len()), (2, 2));
        again.compact().unwrap();
        assert_eq!(ops_files(&dir), [dir.join("ops-2.jsonl")], "compacted into the snapshot; the old ops are gone");
        assert_eq!(fs::metadata(dir.join("ops-2.jsonl")).unwrap().len(), 0);
        again.persist(&DurableOp::Append(vec![noop(2, 3)])).unwrap();
        assert_eq!(RaftStore::new(&dir).load().unwrap().unwrap().log.len(), 3, "appends after compacting");
    }

    #[test]
    fn ops_folded_into_a_snapshot_are_not_replayed_after_a_crash_mid_compaction() {
        let dir = tmp("compact-crash");
        let mut s = RaftStore::new(&dir);
        s.store(&Durable::new([HostId::from("a")].into())).unwrap();
        s.persist(&DurableOp::Append(vec![noop(1, 1), noop(1, 2)])).unwrap();
        let folded = fs::read(dir.join("ops-1.jsonl")).unwrap();
        s.compact().unwrap();
        // As if power went before the old ops file was removed.
        fs::write(dir.join("ops-1.jsonl"), folded).unwrap();
        let got = RaftStore::new(&dir).load().unwrap().unwrap();
        assert_eq!(got.log.iter().map(|e| e.index).collect::<Vec<_>>(), [1, 2], "each entry once");
    }

    #[test]
    fn an_older_builds_store_loads_and_moves_to_generations() {
        let dir = tmp("legacy");
        fs::create_dir_all(&dir).unwrap();
        let start: Durable<LogCmd> = Durable::new([HostId::from("a")].into());
        fs::write(dir.join("snapshot.json"), serde_json::to_vec(&start).unwrap()).unwrap();
        let op: DurableOp<LogCmd> = DurableOp::SetHardState { term: 4, voted_for: None };
        fs::write(dir.join("ops.jsonl"), [serde_json::to_vec(&op).unwrap(), b"\n".to_vec()].concat()).unwrap();
        let mut s = RaftStore::new(&dir);
        assert_eq!(s.load().unwrap().unwrap().term, 4);
        s.compact().unwrap();
        assert_eq!(ops_files(&dir), [dir.join("ops-1.jsonl")]);
        assert_eq!(RaftStore::new(&dir).load().unwrap().unwrap().term, 4);
    }
}
