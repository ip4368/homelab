//! Health: hosts heartbeat to the leader. Silence beyond [`NOT_READY_AFTER_MS`]
//! marks a host `NotReady` and raises a `HostDown` alert; a heartbeat from a
//! `NotReady` host recovers it and resolves the alert.

use super::{Action, Volatile};
use crate::runtime::SimTime;
use crate::state::{Alert, ClusterState, Command, HostObj, Lifecycle, ObjectRef, Severity};

pub const NOT_READY_AFTER_MS: u64 = 40_000;
pub const HOST_DOWN: &str = "HostDown";

pub fn run(state: &ClusterState, v: &Volatile, now: SimTime) -> Vec<Action> {
    let mut out = vec![];
    for host in state.hosts.values() {
        // Hosts still joining or provisioning are not expected to heartbeat yet.
        if !matches!(
            host.lifecycle,
            Lifecycle::Ready | Lifecycle::Cordoned | Lifecycle::Draining | Lifecycle::Drained | Lifecycle::NotReady
        ) {
            continue;
        }
        let id = &host.id;
        let last_seen = v.last_seen(id);
        let silent = now.saturating_sub(last_seen) > NOT_READY_AFTER_MS;
        let open = open_alert(state, host);
        if silent {
            if host.lifecycle != Lifecycle::NotReady {
                out.push(Action::Propose(Command::SetLifecycle { id: id.clone(), lifecycle: Lifecycle::NotReady }));
            }
            if open.is_none() {
                out.push(Action::Propose(Command::RaiseAlert {
                    // Unique per incident and stable across runs: an incident is
                    // identified by the last time the host was seen.
                    id: format!("hostdown-{id}-{last_seen}"),
                    severity: Severity::Critical,
                    kind: HOST_DOWN.into(),
                    entity: ObjectRef::host(id),
                    message: format!("{id} sent no heartbeat for {} s", NOT_READY_AFTER_MS / 1000),
                    ground_truth: false,
                }));
            }
        } else if v.last_heartbeat.contains_key(id) {
            // Only a real heartbeat counts as recovery, not the election grace.
            if host.lifecycle == Lifecycle::NotReady {
                out.push(Action::Propose(Command::SetLifecycle { id: id.clone(), lifecycle: Lifecycle::Ready }));
            }
            if let Some(a) = open {
                out.push(Action::Propose(Command::ResolveAlert { id: a.id.clone() }));
            }
        }
    }
    out
}

fn open_alert<'a>(state: &'a ClusterState, host: &HostObj) -> Option<&'a Alert> {
    let entity = ObjectRef::host(&host.id);
    state.alerts.values().find(|a| a.kind == HOST_DOWN && a.entity == entity && a.resolved_at.is_none())
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::{profile, HostId, Inventory, ProfileId, Role};

    fn hid(s: &str) -> HostId {
        HostId::from(s)
    }

    /// Hosts walked to the given lifecycle via Provisioning and Ready.
    fn state_with(hosts: &[(&str, Lifecycle)]) -> ClusterState {
        let mut s = ClusterState::new();
        for &(h, lc) in hosts {
            let id = hid(h);
            let p = ProfileId::MiniPc;
            s.apply(0, Command::AddPendingHost {
                id: id.clone(),
                profile: profile(p, 0),
                inventory: Inventory::from_profile(p, 0),
                location: "rack-1".into(),
            })
            .unwrap();
            if lc == Lifecycle::Pending {
                continue;
            }
            s.apply(0, Command::ApproveHost { id: id.clone(), roles: [Role::Compute].into() }).unwrap();
            for step in [Lifecycle::Provisioning, Lifecycle::Ready, lc] {
                if s.hosts[&id].lifecycle != lc {
                    s.apply(0, Command::SetLifecycle { id: id.clone(), lifecycle: step }).unwrap();
                }
            }
        }
        s
    }

    fn proposals(actions: Vec<Action>) -> Vec<Command> {
        actions
            .into_iter()
            .map(|a| match a {
                Action::Propose(c) => c,
                other => panic!("unexpected {other:?}"),
            })
            .collect()
    }

    fn apply_all(s: &mut ClusterState, at: SimTime, cmds: Vec<Command>) {
        for c in cmds {
            s.apply(at, c).unwrap();
        }
    }

    #[test]
    fn new_leader_gives_every_host_a_grace_period() {
        let s = state_with(&[("h1", Lifecycle::Ready), ("h2", Lifecycle::Cordoned)]);
        let v = Volatile::new(100_000);
        assert!(run(&s, &v, 100_000 + NOT_READY_AFTER_MS).is_empty(), "no mass-fail on takeover");
        let cmds = proposals(run(&s, &v, 100_000 + NOT_READY_AFTER_MS + 1));
        assert_eq!(cmds.len(), 4, "both hosts go NotReady with an alert each: {cmds:?}");
    }

    #[test]
    fn silent_host_goes_not_ready_with_one_alert_then_recovers() {
        let mut s = state_with(&[("h1", Lifecycle::Draining), ("h2", Lifecycle::Ready)]);
        let mut v = Volatile::new(0);
        v.last_heartbeat.insert(hid("h1"), 5_000);
        v.last_heartbeat.insert(hid("h2"), 44_000);
        let now = 46_000;
        let cmds = proposals(run(&s, &v, now));
        assert_eq!(
            cmds,
            vec![
                Command::SetLifecycle { id: hid("h1"), lifecycle: Lifecycle::NotReady },
                Command::RaiseAlert {
                    id: "hostdown-h1-5000".into(),
                    severity: Severity::Critical,
                    kind: HOST_DOWN.into(),
                    entity: ObjectRef::host(&hid("h1")),
                    message: "h1 sent no heartbeat for 40 s".into(),
                    ground_truth: false,
                },
            ]
        );
        assert_eq!(proposals(run(&s, &v, now + 1000)), cmds, "same incident, same commands (deduped in flight)");
        apply_all(&mut s, now, cmds);
        assert!(run(&s, &v, now + 2000).is_empty(), "nothing more once applied");

        v.last_heartbeat.insert(hid("h1"), now + 3000);
        let cmds = proposals(run(&s, &v, now + 3000));
        assert_eq!(
            cmds,
            vec![
                Command::SetLifecycle { id: hid("h1"), lifecycle: Lifecycle::Ready },
                Command::ResolveAlert { id: "hostdown-h1-5000".into() },
            ]
        );
        apply_all(&mut s, now + 3000, cmds);
        assert_eq!(s.hosts[&hid("h1")].lifecycle, Lifecycle::Draining, "recovery restores the prior state");
        assert!(run(&s, &v, now + 4000).is_empty());
    }

    #[test]
    fn hosts_not_yet_ready_are_ignored() {
        let s = state_with(&[("h1", Lifecycle::Pending), ("h2", Lifecycle::Provisioning)]);
        assert!(run(&s, &Volatile::new(0), 10 * NOT_READY_AFTER_MS).is_empty());
    }

    #[test]
    fn not_ready_host_from_previous_leader_stays_down_until_it_heartbeats() {
        let mut s = state_with(&[("h1", Lifecycle::Ready)]);
        s.apply(1, Command::SetLifecycle { id: hid("h1"), lifecycle: Lifecycle::NotReady }).unwrap();
        let v = Volatile::new(50_000);
        assert!(run(&s, &v, 60_000).is_empty(), "election grace is not a heartbeat");
        // Still silent past the grace: the incident had no alert yet, so raise one.
        let cmds = proposals(run(&s, &v, 95_000));
        assert!(matches!(cmds.as_slice(), [Command::RaiseAlert { id, .. }] if id == "hostdown-h1-50000"));
    }
}
