//! `GET /api/ws`: the live view. A connection gets a snapshot first, then a
//! delta each time the driver publishes. Deltas are computed per connection
//! from the view version it was last sent, so a slow reader just gets a
//! bigger delta, never a stale one.

use crate::driver::{ClockInfo, DriverHandle, Published};
use crate::kube::KubeView;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use control::proto::{KubeHealth, MetricSample};
use control::state::{
    Alert, AutoApproveRule, CpInfo, Event, HostObj, JoinToken, NetworkObj, Object, ObjectRef, Rollout, SwitchObj, TemplateObj,
};
use model::HostId;
use serde::Serialize;
use lab::manager::LabTruth;
use std::collections::BTreeMap;
use std::sync::Arc;
use ts_rs::TS;

/// The client's whole view of the cluster, as lists.
#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[ts(export)]
pub struct ViewSnapshot {
    #[ts(type = "number")]
    pub version: u64,
    pub leader: Option<HostId>,
    #[ts(type = "number")]
    pub term: u64,
    #[ts(type = "number | null")]
    pub last_sync_at: Option<u64>,
    pub hosts: Vec<HostObj>,
    pub tokens: Vec<JoinToken>,
    pub rules: Vec<AutoApproveRule>,
    pub switches: Vec<SwitchObj>,
    pub networks: Vec<NetworkObj>,
    pub templates: Vec<TemplateObj>,
    pub rollouts: Vec<Rollout>,
    pub alerts: Vec<Alert>,
    pub events: Vec<Event>,
    pub cp: CpInfo,
    /// Latest sample per host.
    pub metrics: BTreeMap<HostId, MetricSample>,
    /// Latest K3s API health per server.
    pub kube_health: BTreeMap<HostId, KubeHealth>,
}

impl ViewSnapshot {
    fn of(p: &Published) -> Self {
        let (v, s) = (&p.view, &p.view.state);
        Self {
            version: v.version,
            leader: v.leader.clone(),
            term: v.term,
            last_sync_at: v.last_sync_at,
            hosts: s.hosts.values().cloned().collect(),
            tokens: s.tokens.values().cloned().collect(),
            rules: s.rules.values().cloned().collect(),
            switches: s.switches.values().cloned().collect(),
            networks: s.networks.values().cloned().collect(),
            templates: s.templates.values().cloned().collect(),
            rollouts: s.rollouts.values().cloned().collect(),
            alerts: s.alerts.values().cloned().collect(),
            events: s.events.values().cloned().collect(),
            cp: s.cp.clone(),
            metrics: v.metrics.clone(),
            kube_health: v.kube_health.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
#[ts(export)]
pub enum ServerMsg {
    /// Replaces everything the UI holds.
    Snapshot { view: ViewSnapshot, truth: LabTruth, kube: KubeView, clock: ClockInfo },
    /// What changed since the previous message. `truth` and `clock` are always whole.
    Delta {
        #[ts(type = "number")]
        version: u64,
        leader: Option<HostId>,
        #[ts(type = "number")]
        term: u64,
        #[ts(type = "number | null")]
        last_sync_at: Option<u64>,
        upserts: Vec<Object>,
        removed: Vec<ObjectRef>,
        /// Samples newer than the last ones sent.
        metrics: BTreeMap<HostId, MetricSample>,
        /// Health that changed since it was last sent.
        kube_health: BTreeMap<HostId, KubeHealth>,
        truth: LabTruth,
        /// What Kubernetes runs, if it was read again since the previous message.
        kube: Option<KubeView>,
        clock: ClockInfo,
    },
}

/// What one connection has been sent so far.
pub struct Connection {
    epoch: u64,
    version: u64,
    /// When each host's latest sample was taken, as last sent.
    metrics_sent: BTreeMap<HostId, u64>,
    /// Each server's K3s API health, as last sent.
    health_sent: BTreeMap<HostId, KubeHealth>,
    /// The Kubernetes view last sent.
    kube_sent: Arc<KubeView>,
}

impl Connection {
    /// Starts a connection with its snapshot of `p`.
    pub fn open(p: &Published) -> (Self, ServerMsg) {
        let mut c = Self { epoch: p.epoch, version: 0, metrics_sent: BTreeMap::new(), health_sent: BTreeMap::new(), kube_sent: p.kube.clone() };
        let msg = c.snapshot(p);
        (c, msg)
    }

    fn snapshot(&mut self, p: &Published) -> ServerMsg {
        self.epoch = p.epoch;
        self.version = p.view.version;
        self.metrics_sent = p.view.metrics.iter().map(|(h, m)| (h.clone(), m.at)).collect();
        self.health_sent = p.view.kube_health.clone();
        self.kube_sent = p.kube.clone();
        ServerMsg::Snapshot { view: ViewSnapshot::of(p), truth: p.truth.clone(), kube: (*p.kube).clone(), clock: p.clock }
    }

    /// The message that brings this connection up to `p`: a delta, or a fresh
    /// snapshot if the client was replaced. The epoch says so; a view that went
    /// back in time is taken as a sign of it too.
    pub fn next(&mut self, p: &Published) -> ServerMsg {
        if p.epoch != self.epoch || p.view.version < self.version {
            return self.snapshot(p);
        }
        let changes = p.view.state.changes_since(self.version);
        let metrics: BTreeMap<_, _> = p
            .view
            .metrics
            .iter()
            .filter(|(h, m)| self.metrics_sent.get(*h).is_none_or(|&at| m.at > at))
            .map(|(h, m)| (h.clone(), m.clone()))
            .collect();
        for (h, m) in &metrics {
            self.metrics_sent.insert(h.clone(), m.at);
        }
        let kube_health: BTreeMap<_, _> = p
            .view
            .kube_health
            .iter()
            .filter(|(h, hh)| self.health_sent.get(*h) != Some(*hh))
            .map(|(h, hh)| (h.clone(), hh.clone()))
            .collect();
        for (h, hh) in &kube_health {
            self.health_sent.insert(h.clone(), hh.clone());
        }
        self.version = p.view.version;
        let kube = (!Arc::ptr_eq(&self.kube_sent, &p.kube)).then(|| (*p.kube).clone());
        self.kube_sent = p.kube.clone();
        ServerMsg::Delta {
            version: changes.version,
            leader: p.view.leader.clone(),
            term: p.view.term,
            last_sync_at: p.view.last_sync_at,
            upserts: changes.upserts,
            removed: changes.removed,
            metrics,
            kube_health,
            truth: p.truth.clone(),
            kube,
            clock: p.clock,
        }
    }
}

pub async fn ws_handler(State(driver): State<DriverHandle>, upgrade: WebSocketUpgrade) -> Response {
    upgrade.on_upgrade(move |socket| serve(socket, driver))
}

async fn send(socket: &mut WebSocket, msg: &ServerMsg) -> bool {
    let json = serde_json::to_string(msg).expect("server messages serialize");
    socket.send(Message::Text(json.into())).await.is_ok()
}

async fn serve(mut socket: WebSocket, driver: DriverHandle) {
    let mut published = driver.subscribe();
    let first = published.borrow_and_update().clone();
    let (mut conn, msg) = Connection::open(&first);
    if !send(&mut socket, &msg).await {
        return;
    }
    loop {
        tokio::select! {
            changed = published.changed() => {
                if changed.is_err() {
                    return;
                }
                let p = published.borrow_and_update().clone();
                if !send(&mut socket, &conn.next(&p)).await {
                    return;
                }
            }
            incoming = socket.recv() => match incoming {
                // The UI sends nothing; anything but a close or error is ignored.
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return,
                Some(Ok(_)) => {}
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use control::client::ClusterView;
    use control::state::{ClusterState, Command, ObjectKind};
    use model::{Inventory, ProfileId, profile};

    fn add(leader: &mut ClusterState, id: &str) {
        let cmd = Command::AddPendingHost {
            id: HostId::from(id),
            profile: profile(ProfileId::MiniPc, 0),
            inventory: Inventory::from_profile(ProfileId::MiniPc, 0),
            location: "rack-1".into(),
        };
        leader.apply(1, cmd).unwrap();
    }

    fn published(view: &ClusterView, now: u64) -> Published {
        let truth = LabTruth { vms: vec![], switches: vec![], ports: vec![], partitions: vec![], vip_holders: vec![], at: now };
        Published { epoch: 0, view: view.clone(), truth, kube: Arc::new(KubeView::default()), clock: ClockInfo { now } }
    }

    fn sample(at: u64) -> MetricSample {
        MetricSample {
            at,
            cpu_pct: 1.0,
            ram_pct: 1.0,
            temp_c: Some(1.0),
            disk_pct: BTreeMap::new(),
            port_rx_bps: BTreeMap::new(),
            port_tx_bps: BTreeMap::new(),
        }
    }

    #[test]
    fn deltas_carry_only_what_this_connection_has_not_seen() {
        let mut leader = ClusterState::new();
        let mut view = ClusterView::default();
        add(&mut leader, "h1");
        add(&mut leader, "h2");
        view.merge(leader.changes_since(0));
        let (mut conn, first) = Connection::open(&published(&view, 10));
        let ServerMsg::Snapshot { view: snap, clock, .. } = first else { panic!("{first:?}") };
        assert_eq!((snap.hosts.len(), snap.version, clock.now), (2, 2, 10));

        // Nothing changed: still a message, so the clock ticks.
        let ServerMsg::Delta { upserts, removed, clock, .. } = conn.next(&published(&view, 20)) else { panic!() };
        assert_eq!((upserts.len(), removed.len(), clock.now), (0, 0, 20));

        let labels = [("a".into(), "b".into())].into();
        leader.apply(2, Command::SetLabels { id: HostId::from("h1"), labels }).unwrap();
        leader.apply(3, Command::RemoveHost { id: HostId::from("h2") }).unwrap();
        view.merge(leader.changes_since(view.version));
        let ServerMsg::Delta { upserts, removed, version, .. } = conn.next(&published(&view, 30)) else { panic!() };
        assert_eq!(version, 4);
        assert!(matches!(upserts.as_slice(), [Object::Host(h)] if h.id == HostId::from("h1")), "{upserts:?}");
        assert_eq!(removed, [ObjectRef { kind: ObjectKind::Host, id: "h2".into() }]);

        // The removal is not repeated.
        let ServerMsg::Delta { removed, upserts, .. } = conn.next(&published(&view, 40)) else { panic!() };
        assert!(removed.is_empty() && upserts.is_empty());
    }

    #[test]
    fn health_travels_in_snapshots_and_only_changes_in_deltas() {
        let mut view = ClusterView::default();
        view.kube_health.insert(HostId::from("h1"), KubeHealth { api_ready: true });
        let (mut conn, first) = Connection::open(&published(&view, 10));
        let ServerMsg::Snapshot { view: snap, .. } = first else { panic!("{first:?}") };
        assert_eq!(snap.kube_health.get(&HostId::from("h1")), Some(&KubeHealth { api_ready: true }));

        let ServerMsg::Delta { kube_health, .. } = conn.next(&published(&view, 20)) else { panic!() };
        assert!(kube_health.is_empty(), "unchanged health is not repeated");

        view.kube_health.insert(HostId::from("h1"), KubeHealth { api_ready: false });
        let ServerMsg::Delta { kube_health, .. } = conn.next(&published(&view, 30)) else { panic!() };
        assert_eq!(kube_health.get(&HostId::from("h1")), Some(&KubeHealth { api_ready: false }));
    }

    #[test]
    fn switches_travel_in_snapshots_and_deltas() {
        let observe = |last_seen| Command::ObserveSwitch {
            id: model::SwitchId::from("access-1"),
            model: "m".into(),
            ports: vec![],
            mlag_peer: None,
            last_seen,
        };
        let mut leader = ClusterState::new();
        let mut view = ClusterView::default();
        leader.apply(1, observe(1)).unwrap();
        view.merge(leader.changes_since(0));
        let (mut conn, first) = Connection::open(&published(&view, 10));
        let ServerMsg::Snapshot { view: snap, .. } = first else { panic!("{first:?}") };
        assert_eq!(snap.switches.len(), 1);

        leader.apply(2, observe(61_000)).unwrap();
        view.merge(leader.changes_since(view.version));
        let ServerMsg::Delta { upserts, .. } = conn.next(&published(&view, 20)) else { panic!() };
        assert!(matches!(upserts.as_slice(), [Object::Switch(s)] if s.last_seen == 61_000), "{upserts:?}");
    }

    #[test]
    fn networks_templates_and_rollouts_travel_in_snapshots() {
        let mut leader = ClusterState::new();
        let mut view = ClusterView::default();
        for n in model::seed_networks() {
            leader.apply(1, Command::UpsertNetwork { network: n }).unwrap();
        }
        for t in model::seed_templates() {
            leader.apply(1, Command::UpsertTemplate { template: t }).unwrap();
        }
        view.merge(leader.changes_since(0));
        let (_, first) = Connection::open(&published(&view, 10));
        let ServerMsg::Snapshot { view: snap, .. } = first else { panic!("{first:?}") };
        assert_eq!((snap.networks.len(), snap.templates.len(), snap.rollouts.len()), (5, 3, 0));
    }

    #[test]
    fn metrics_are_sent_once_per_new_sample() {
        let mut view = ClusterView::default();
        view.metrics.insert(HostId::from("h1"), sample(5));
        let (mut conn, first) = Connection::open(&published(&view, 10));
        let ServerMsg::Snapshot { view: snap, .. } = first else { panic!() };
        assert_eq!(snap.metrics.len(), 1);

        let metrics = |m: ServerMsg| match m {
            ServerMsg::Delta { metrics, .. } => metrics,
            other => panic!("{other:?}"),
        };
        assert!(metrics(conn.next(&published(&view, 20))).is_empty());
        view.metrics.insert(HostId::from("h1"), sample(15));
        view.metrics.insert(HostId::from("h2"), sample(16));
        assert_eq!(metrics(conn.next(&published(&view, 30))).len(), 2);
        assert!(metrics(conn.next(&published(&view, 40))).is_empty());
    }

    #[test]
    fn a_view_that_went_back_in_time_is_sent_as_a_new_snapshot() {
        let mut leader = ClusterState::new();
        let mut view = ClusterView::default();
        add(&mut leader, "h1");
        add(&mut leader, "h2");
        view.merge(leader.changes_since(0));
        let (mut conn, _) = Connection::open(&published(&view, 10));

        // A preset was loaded: the new client starts again from a younger leader state.
        let mut fresh = ClusterState::new();
        add(&mut fresh, "x1");
        let mut new_view = ClusterView::default();
        new_view.merge(fresh.changes_since(0));
        let ServerMsg::Snapshot { view: snap, .. } = conn.next(&published(&new_view, 20)) else { panic!("expected a snapshot") };
        assert_eq!(snap.hosts.len(), 1);
        assert!(matches!(conn.next(&published(&new_view, 30)), ServerMsg::Delta { .. }));
    }

    #[test]
    fn a_new_epoch_is_a_new_snapshot_even_if_the_view_looks_newer() {
        let mut leader = ClusterState::new();
        let mut view = ClusterView::default();
        add(&mut leader, "h1");
        view.merge(leader.changes_since(0));
        let (mut conn, _) = Connection::open(&published(&view, 10));

        // The reloaded fleet's view is already past the old version by the time it is published.
        let mut fresh = ClusterState::new();
        for id in ["x1", "x2", "x3"] {
            add(&mut fresh, id);
        }
        let mut new_view = ClusterView::default();
        new_view.merge(fresh.changes_since(0));
        assert!(new_view.version > view.version);
        let p = Published { epoch: 1, ..published(&new_view, 20) };
        let ServerMsg::Snapshot { view: snap, .. } = conn.next(&p) else { panic!("expected a snapshot") };
        assert_eq!(snap.hosts.len(), 3);
        assert!(matches!(conn.next(&p), ServerMsg::Delta { .. }), "one snapshot per epoch");
    }

    #[test]
    fn the_kubernetes_view_is_sent_once_per_read() {
        let view = ClusterView::default();
        let read = |at| Arc::new(KubeView { at, ..KubeView::default() });
        let p = Published { kube: read(5), ..published(&view, 10) };
        let (mut conn, first) = Connection::open(&p);
        let ServerMsg::Snapshot { kube, .. } = first else { panic!() };
        assert_eq!(kube.at, 5);
        let kube_of = |m: ServerMsg| match m {
            ServerMsg::Delta { kube, .. } => kube,
            other => panic!("{other:?}"),
        };
        assert_eq!(kube_of(conn.next(&p)), None, "the same read");
        let p = Published { kube: read(8), ..p };
        assert_eq!(kube_of(conn.next(&p)).map(|k| k.at), Some(8));
        assert_eq!(kube_of(conn.next(&p)), None);
    }

    #[test]
    fn deltas_carry_the_sync_state() {
        let mut view = ClusterView::default();
        let (mut conn, _) = Connection::open(&published(&view, 10));
        view.last_sync_at = Some(7);
        view.term = 3;
        view.leader = Some(HostId::from("h1"));
        let ServerMsg::Delta { last_sync_at, term, leader, .. } = conn.next(&published(&view, 20)) else { panic!() };
        assert_eq!((last_sync_at, term, leader), (Some(7), 3, Some(HostId::from("h1"))));
    }
}
