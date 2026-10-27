//! The host agent: the daemon on every machine. An unjoined agent boots its
//! host on the bootstrap network config and asks the control plane (via the
//! VIP) to join with a token until it is accepted. A joined agent heartbeats,
//! reports metrics, and applies the network configs the leader pushes, like
//! `netplan try`: a config that is not confirmed within [`REVERT_MS`] (because
//! it cut the host off from the control plane) is rolled back, and only a
//! confirmed one is written for the next boot. A member also runs the
//! instances and holds the volume replicas the leader asks for, and lists
//! them in every heartbeat. As a live migration's target it watches each
//! incoming instance: it reports `MigrationDone` once the instance runs, and
//! gives it up if it has not by the deadline the leader set.

use crate::proto::{mgmt, InstanceSpec, MetricSample, Msg, Observed};
use crate::runtime::{Actor, Addr, Ctx, Endpoint, SimTime, Svc};
use crate::state::NetworkResult;
use model::network::{bootstrap_config, HostNetworkConfig, LogicalNetwork};
use model::{profile, HostId, Inventory};
use std::collections::BTreeMap;

pub const JOIN_RETRY_MS: u64 = 5000;
pub const HEARTBEAT_MS: u64 = 5000;
pub const METRICS_MS: u64 = 10_000;
/// How long a newly applied network config waits for the leader's confirmation.
pub const REVERT_MS: u64 = 30_000;
/// How often the agent looks in on the instances migrating in.
pub const MIGRATION_POLL_MS: u64 = 1000;
/// Synthetic temperature: idle plus a share of CPU load.
const IDLE_TEMP_C: f32 = 35.0;
const TEMP_C_PER_CPU_PCT: f32 = 0.4;

const VIP: Endpoint = Endpoint { addr: Addr::Vip, svc: Svc::Cp };

const JOIN: u64 = 0;
const HEARTBEAT: u64 = 1;
const METRICS: u64 = 2;
const REVERT: u64 = 3;
const MIGRATION: u64 = 4;

/// A network config applied but not yet confirmed, and what to go back to.
#[derive(Debug)]
struct Trial {
    generation: u64,
    since: SimTime,
    /// The last confirmed config, the network definitions it ran with, and its generation.
    previous: HostNetworkConfig,
    previous_networks: Vec<LogicalNetwork>,
    previous_generation: u64,
}

#[derive(Debug, Default)]
pub struct HostAgent {
    /// `Some` until the control plane accepts the join.
    token: Option<String>,
    /// What the agent last observed about its host, and when that changed.
    observed: Option<(SimTime, Observed)>,
    /// The generation of the network config the host runs, on trial or
    /// confirmed; 0 when unknown (never told, or the agent restarted).
    net_generation: u64,
    trial: Option<Trial>,
    /// The instances migrating in, and the deadline of each.
    incoming: BTreeMap<String, SimTime>,
}

impl HostAgent {
    /// `token` is the join token for an unjoined host; `None` means the host
    /// already joined (e.g. it rebooted) and the agent just heartbeats.
    pub fn new(token: Option<String>) -> Self {
        Self { token, observed: None, net_generation: 0, trial: None, incoming: BTreeMap::new() }
    }

    /// Whether the agent runs as a cluster member (not trying to join).
    pub fn is_member(&self) -> bool {
        self.token.is_none()
    }

    /// The generation of the network config the host runs (0 if unknown).
    pub fn net_generation(&self) -> u64 {
        self.net_generation
    }

    fn send_join(&self, ctx: &mut dyn Ctx) {
        let (Some(token), Some(os)) = (&self.token, ctx.host()) else { return };
        let msg = Msg::JoinRequest { token: token.clone(), inventory: os.inventory() };
        ctx.send(VIP, mgmt(), msg);
    }

    /// Heartbeats the host's liveness and network generation. The
    /// observation (LLDP and inventory) rides along only when it changed
    /// since the last one, on the first heartbeat after start, or when
    /// `report` asks for it; every heartbeat carries the observation's stamp
    /// so the leader can tell if it has it.
    fn send_heartbeat(&mut self, ctx: &mut dyn Ctx, report: bool) {
        let Addr::Host(host) = ctx.me().addr else { return };
        let now = ctx.now();
        let (current, instances, replicas) = match ctx.host() {
            Some(os) => (Some(Observed { lldp: os.lldp(), inventory: os.inventory() }), os.instances(), os.replicas()),
            None => (None, vec![], vec![]),
        };
        let mut report = report;
        if let Some(current) = current
            && self.observed.as_ref().is_none_or(|(_, seen)| *seen != current)
        {
            self.observed = Some((now, current));
            report = true;
        }
        let (observed_at, observed) = match &self.observed {
            Some((at, o)) => (Some(*at), report.then(|| o.clone())),
            None => (None, None),
        };
        let net_generation = self.net_generation;
        let hb = Msg::Heartbeat { host, lifecycle_seen: 0, observed_at, observed, net_generation, instances, replicas };
        ctx.send(VIP, mgmt(), hb);
    }

    fn send_metrics(&self, ctx: &mut dyn Ctx) {
        let now = ctx.now();
        let Some(os) = ctx.host() else { return };
        let (host, usage, instances) = (os.host_id(), os.usage(), os.instances().len());
        let sample = MetricSample {
            at: now,
            cpu_pct: usage.cpu_pct,
            ram_pct: usage.ram_pct,
            temp_c: IDLE_TEMP_C + TEMP_C_PER_CPU_PCT * usage.cpu_pct,
            disk_pct: usage.disk_pct,
            port_rx_bps: usage.port_bps.iter().map(|(p, &(rx, _))| (p.clone(), rx)).collect(),
            port_tx_bps: usage.port_bps.into_iter().map(|(p, (_, tx))| (p, tx)).collect(),
            instances: u32::try_from(instances).unwrap_or(u32::MAX),
        };
        ctx.send(VIP, mgmt(), Msg::Metrics { host, sample });
    }

    /// Stops asking to join, records membership on the host, and starts
    /// heartbeating (right away) and reporting metrics.
    fn start_member(&mut self, ctx: &mut dyn Ctx) {
        self.token = None;
        if let Some(os) = ctx.host() {
            os.mark_joined();
        }
        self.send_heartbeat(ctx, false);
        ctx.set_timer(HEARTBEAT_MS, HEARTBEAT);
        ctx.set_timer(METRICS_MS, METRICS);
    }

    /// Applies `config` on trial and reports `Applied` over it; the leader's
    /// `ConfirmNetwork` keeps it, silence reverts it ([`Self::revert`]). A
    /// repeat of the running generation is answered again (the report may
    /// have been lost); an older one is ignored.
    fn apply(&mut self, ctx: &mut dyn Ctx, generation: u64, config: HostNetworkConfig, networks: Vec<LogicalNetwork>) {
        if generation < self.net_generation {
            return;
        }
        if generation == self.net_generation {
            return report(ctx, generation, NetworkResult::Applied);
        }
        let now = ctx.now();
        let Some(os) = ctx.host() else { return };
        if let Err(reason) = check_nics(&config, &os.inventory()) {
            return report(ctx, generation, NetworkResult::Failed(reason));
        }
        // A trial superseded before it was confirmed is no fallback: keep the last confirmed config.
        let (previous, previous_networks, previous_generation) = match self.trial.take() {
            Some(t) => (t.previous, t.previous_networks, t.previous_generation),
            None => (os.applied_network(), os.applied_networks(), self.net_generation),
        };
        os.apply_network(config, networks);
        self.trial = Some(Trial { generation, since: now, previous, previous_networks, previous_generation });
        self.net_generation = generation;
        report(ctx, generation, NetworkResult::Applied);
        ctx.set_timer(REVERT_MS, REVERT);
    }

    /// Starts an incoming instance of `spec` from `from`, unless `deadline`
    /// has passed, and watches it until then. One that runs already is
    /// reported again.
    fn migrate_in(&mut self, ctx: &mut dyn Ctx, spec: InstanceSpec, from: HostId, deadline: SimTime) {
        let now = ctx.now();
        let Some(os) = ctx.host() else { return };
        let (id, idle) = (spec.id.clone(), self.incoming.is_empty());
        if now < deadline {
            os.migrate_in(spec, from, deadline);
        }
        if os.migrations().contains(&id) || os.instances().contains(&id) {
            self.incoming.insert(id, deadline);
        }
        if idle && !self.incoming.is_empty() {
            ctx.set_timer(MIGRATION_POLL_MS, MIGRATION);
        }
    }

    /// Reports each watched instance that runs now, gives up each still
    /// migrating in at its deadline, and forgets those stopped meanwhile;
    /// looks again in [`MIGRATION_POLL_MS`] while any is left.
    fn poll_migrations(&mut self, ctx: &mut dyn Ctx) {
        let now = ctx.now();
        let Some(os) = ctx.host() else { return };
        let (running, migrating) = (os.instances(), os.migrations());
        let mut done = vec![];
        self.incoming.retain(|id, deadline| {
            if running.contains(id) {
                done.push(id.clone());
                false
            } else if migrating.contains(id) && now >= *deadline {
                os.abort_migration(id);
                false
            } else {
                migrating.contains(id)
            }
        });
        for instance in done {
            ctx.send(VIP, mgmt(), Msg::MigrationDone { instance });
        }
        if !self.incoming.is_empty() {
            ctx.set_timer(MIGRATION_POLL_MS, MIGRATION);
        }
    }

    /// Puts the last confirmed config back if the trial is still unconfirmed
    /// [`REVERT_MS`] after it started (timers of superseded trials are ignored).
    fn revert(&mut self, ctx: &mut dyn Ctx) {
        let now = ctx.now();
        let Some(t) = self.trial.take_if(|t| now >= t.since + REVERT_MS) else { return };
        if let Some(os) = ctx.host() {
            os.apply_network(t.previous, t.previous_networks);
        }
        self.net_generation = t.previous_generation;
        report(ctx, t.generation, NetworkResult::Reverted);
    }
}

fn report(ctx: &mut dyn Ctx, generation: u64, result: NetworkResult) {
    ctx.send(VIP, mgmt(), Msg::NetworkStatus { generation, result });
}

/// Every port the config's links use must be one of the host's NICs.
fn check_nics(config: &HostNetworkConfig, inventory: &Inventory) -> Result<(), String> {
    let mut ports = config.links.iter().flat_map(|l| l.kind.ports());
    match ports.find(|p| !inventory.nics.iter().any(|n| n.name == p.as_str())) {
        Some(p) => Err(format!("no NIC {p}")),
        None => Ok(()),
    }
}

impl Actor for HostAgent {
    fn on_start(&mut self, ctx: &mut dyn Ctx) {
        if self.token.is_none() {
            return self.start_member(ctx);
        }
        if let Some(os) = ctx.host()
            && os.applied_network().links.is_empty()
        {
            let inv = os.inventory();
            let networks = os.applied_networks();
            os.apply_network(bootstrap_config(&profile(inv.profile, inv.variant)), networks);
        }
        self.send_join(ctx);
        ctx.set_timer(JOIN_RETRY_MS, JOIN);
    }

    fn on_message(&mut self, ctx: &mut dyn Ctx, _from: Endpoint, msg: Msg) {
        match msg {
            Msg::JoinResp(Ok(())) if self.token.is_some() => self.start_member(ctx),
            Msg::ApplyNetwork { generation, config, networks } => {
                // Only members are told their config: this proves the join, if its answer was lost.
                if self.token.is_some() {
                    self.start_member(ctx);
                }
                self.apply(ctx, generation, config, networks);
            }
            Msg::ConfirmNetwork { generation } => {
                // Like `netplan try` accepted: only now is the config written for the next boot.
                if self.trial.take_if(|t| t.generation == generation).is_some()
                    && let Some(os) = ctx.host()
                {
                    os.persist_network();
                }
            }
            Msg::ReportRequest if self.token.is_none() => self.send_heartbeat(ctx, true),
            // Workloads are only ever placed on members.
            Msg::StartInstance(spec) if self.token.is_none() => {
                if let Some(os) = ctx.host() {
                    os.start_instance(spec);
                }
            }
            Msg::StopInstance(id) if self.token.is_none() => {
                if let Some(os) = ctx.host() {
                    os.stop_instance(&id);
                }
            }
            Msg::MigrateIn { spec, from, deadline } if self.token.is_none() => self.migrate_in(ctx, spec, from, deadline),
            Msg::AbortMigration(id) if self.token.is_none() => {
                if let Some(os) = ctx.host() {
                    os.abort_migration(&id);
                }
                self.incoming.remove(&id);
            }
            Msg::CreateReplica { volume, size_gb, source } if self.token.is_none() => {
                if let Some(os) = ctx.host() {
                    os.create_replica(&volume, size_gb, source);
                }
            }
            Msg::DeleteReplica { volume } if self.token.is_none() => {
                if let Some(os) = ctx.host() {
                    os.delete_replica(&volume);
                }
            }
            Msg::StartCpReplica { voters } if self.token.is_none() => {
                if let Some(os) = ctx.host() {
                    os.spawn_cp_replica(voters);
                }
            }
            _ => {}
        }
    }

    fn on_timer(&mut self, ctx: &mut dyn Ctx, token: u64) {
        match token {
            JOIN if self.token.is_some() => {
                self.send_join(ctx);
                ctx.set_timer(JOIN_RETRY_MS, JOIN);
            }
            HEARTBEAT => {
                self.send_heartbeat(ctx, false);
                ctx.set_timer(HEARTBEAT_MS, HEARTBEAT);
            }
            METRICS => {
                self.send_metrics(ctx);
                ctx.set_timer(METRICS_MS, METRICS);
            }
            REVERT => self.revert(ctx),
            MIGRATION => self.poll_migrations(ctx),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{FlowEnd, FlowKind, FlowSpec, InstanceSpec, LogCmd, ReplicaReport, UsageProfile};
    use crate::raft::{Durable, DurableOp};
    use crate::runtime::{HostOs, HostUsage, SimTime};
    use crate::state::NetworkResult;
    use model::network::{HostNetworkConfig, LinkKind, LldpNeighbor};
    use model::{HostId, Inventory, NetworkId, PortName, ProfileId, SwitchId};
    use std::collections::BTreeSet;

    struct FakeOs {
        net: HostNetworkConfig,
        /// The network definitions `net` was applied with.
        networks: Vec<LogicalNetwork>,
        /// What the host boots with.
        boot: HostNetworkConfig,
        joined: bool,
        lldp: Vec<LldpNeighbor>,
        inventory: Inventory,
        instances: Vec<InstanceSpec>,
        replicas: Vec<ReplicaReport>,
        /// The voters of every CP replica started.
        cp_spawned: Vec<BTreeSet<HostId>>,
        /// Instances migrating in, and from where.
        incoming: Vec<(InstanceSpec, HostId)>,
    }

    impl HostOs for FakeOs {
        fn host_id(&self) -> HostId {
            HostId::from("m1")
        }
        fn inventory(&self) -> Inventory {
            self.inventory.clone()
        }
        fn lldp(&self) -> Vec<LldpNeighbor> {
            self.lldp.clone()
        }
        fn apply_network(&mut self, cfg: HostNetworkConfig, networks: Vec<LogicalNetwork>) {
            self.net = cfg;
            self.networks = networks;
        }
        fn applied_network(&self) -> HostNetworkConfig {
            self.net.clone()
        }
        fn applied_networks(&self) -> Vec<LogicalNetwork> {
            self.networks.clone()
        }
        fn persist_network(&mut self) {
            self.boot = self.net.clone();
        }
        fn usage(&mut self) -> HostUsage {
            HostUsage {
                cpu_pct: 10.0,
                ram_pct: 20.0,
                disk_pct: [("nvme0".into(), 5.0)].into(),
                port_bps: [("eth0".into(), (1.0, 2.0))].into(),
            }
        }
        fn load_cp(&self) -> Option<Durable<LogCmd>> {
            None
        }
        fn store_cp(&mut self, _: Durable<LogCmd>) {}
        fn persist_cp(&mut self, _: DurableOp<LogCmd>) {}
        fn mark_joined(&mut self) {
            self.joined = true;
        }
        fn start_instance(&mut self, spec: InstanceSpec) {
            self.instances.retain(|i| i.id != spec.id);
            self.instances.push(spec);
        }
        fn stop_instance(&mut self, id: &str) {
            self.instances.retain(|i| i.id != id);
            self.incoming.retain(|(i, _)| i.id != id);
        }
        fn migrate_in(&mut self, spec: InstanceSpec, from: HostId, _deadline: SimTime) {
            if !self.instances.iter().chain(self.incoming.iter().map(|(i, _)| i)).any(|i| i.id == spec.id) {
                self.incoming.push((spec, from));
            }
        }
        fn abort_migration(&mut self, id: &str) {
            self.incoming.retain(|(i, _)| i.id != id);
        }
        fn migrations(&self) -> Vec<String> {
            self.incoming.iter().map(|(i, _)| i.id.clone()).collect()
        }
        fn instances(&self) -> Vec<String> {
            self.instances.iter().map(|i| i.id.clone()).collect()
        }
        fn create_replica(&mut self, volume: &str, _: u32, source: Option<HostId>) -> Option<String> {
            let pct = if source.is_some() { 0 } else { 100 };
            self.replicas.push(ReplicaReport { volume: volume.into(), disk: "nvme0".into(), pct, copied_bytes: 0, copying: pct < 100 });
            Some("nvme0".into())
        }
        fn replicas(&self) -> Vec<ReplicaReport> {
            self.replicas.clone()
        }
        fn delete_replica(&mut self, volume: &str) {
            self.replicas.retain(|r| r.volume != volume);
        }
        fn spawn_cp_replica(&mut self, voters: BTreeSet<HostId>) {
            self.cp_spawned.push(voters);
        }
    }

    /// Records what the agent sends; timers fire when the test advances time.
    struct FakeCtx {
        now: SimTime,
        os: FakeOs,
        sent: Vec<Msg>,
        timers: Vec<(SimTime, u64)>,
    }

    impl FakeCtx {
        fn new(net: HostNetworkConfig) -> Self {
            let os = FakeOs {
                boot: net.clone(),
                net,
                networks: vec![],
                joined: false,
                lldp: vec![neighbor("sw-1", "p1")],
                inventory: Inventory::from_profile(ProfileId::Pi5, 1),
                instances: vec![],
                replicas: vec![],
                cp_spawned: vec![],
                incoming: vec![],
            };
            Self { now: 0, os, sent: vec![], timers: vec![] }
        }

        /// Fires every timer due by `to`, in time order.
        fn advance(&mut self, agent: &mut HostAgent, to: SimTime) {
            while let Some(i) = (0..self.timers.len()).filter(|&i| self.timers[i].0 <= to).min_by_key(|&i| self.timers[i])
            {
                let (at, token) = self.timers.remove(i);
                self.now = at;
                agent.on_timer(self, token);
            }
            self.now = to;
        }

        fn take(&mut self) -> Vec<Msg> {
            std::mem::take(&mut self.sent)
        }

        /// Like `take`, but only the heartbeats (metrics ride on their own timer).
        fn take_heartbeats(&mut self) -> Vec<Msg> {
            self.take().into_iter().filter(|m| matches!(m, Msg::Heartbeat { .. })).collect()
        }

        /// Like `take`, but only the network status reports.
        fn take_statuses(&mut self) -> Vec<Msg> {
            self.take().into_iter().filter(|m| matches!(m, Msg::NetworkStatus { .. })).collect()
        }
    }

    impl Ctx for FakeCtx {
        fn now(&self) -> SimTime {
            self.now
        }
        fn me(&self) -> Endpoint {
            Endpoint::new(Addr::Host(HostId::from("m1")), Svc::Agent)
        }
        fn send(&mut self, to: Endpoint, _: NetworkId, msg: Msg) {
            assert_eq!(to, VIP, "the agent only talks to the VIP");
            self.sent.push(msg);
        }
        fn set_timer(&mut self, after_ms: u64, token: u64) {
            self.timers.push((self.now + after_ms, token));
        }
        fn rand_u64(&mut self) -> u64 {
            0
        }
        fn claim_vip(&mut self) {}
        fn host(&mut self) -> Option<&mut dyn HostOs> {
            Some(&mut self.os)
        }
    }

    fn neighbor(switch: &str, port: &str) -> LldpNeighbor {
        LldpNeighbor { local_port: PortName::from("eth0"), switch: SwitchId::from(switch), switch_port: PortName::from(port), mgmt_ip: None }
    }

    fn observed(os: &FakeOs) -> Observed {
        Observed { lldp: os.lldp.clone(), inventory: os.inventory.clone() }
    }

    /// A heartbeat whose observation was last seen to change at `at`, without the observation.
    fn heartbeat(at: SimTime) -> Msg {
        heartbeat_at_generation(at, 0)
    }

    /// As [`heartbeat`], from a host running network config `generation`.
    fn heartbeat_at_generation(at: SimTime, generation: u64) -> Msg {
        Msg::Heartbeat {
            host: HostId::from("m1"),
            lifecycle_seen: 0,
            observed_at: Some(at),
            observed: None,
            net_generation: generation,
            instances: vec![],
            replicas: vec![],
        }
    }

    /// A heartbeat carrying the observation, which is `os`'s.
    fn reporting(at: SimTime, os: &FakeOs) -> Msg {
        Msg::Heartbeat {
            host: HostId::from("m1"),
            lifecycle_seen: 0,
            observed_at: Some(at),
            observed: Some(observed(os)),
            net_generation: 0,
            instances: vec![],
            replicas: vec![],
        }
    }

    fn join(token: &str) -> Msg {
        Msg::JoinRequest { token: token.into(), inventory: Inventory::from_profile(ProfileId::Pi5, 1) }
    }

    #[test]
    fn unjoined_agent_boots_bootstrap_network_and_retries_join_until_accepted() {
        let mut ctx = FakeCtx::new(HostNetworkConfig::default());
        let mut a = HostAgent::new(Some("tok".into()));
        a.on_start(&mut ctx);
        assert_eq!(ctx.os.net, bootstrap_config(&profile(ProfileId::Pi5, 1)));
        assert_eq!(ctx.take(), [join("tok")]);
        ctx.advance(&mut a, 10_000);
        assert_eq!(ctx.take(), [join("tok"), join("tok")], "every 5 s, nothing else");

        a.on_message(&mut ctx, VIP, Msg::JoinResp(Err("invalid token".into())));
        ctx.advance(&mut a, 15_000);
        assert_eq!(ctx.take(), [join("tok")], "a rejection does not stop the retries");
        assert!(!a.is_member() && !ctx.os.joined);

        a.on_message(&mut ctx, VIP, Msg::JoinResp(Ok(())));
        let first = reporting(15_000, &ctx.os);
        assert_eq!(ctx.take(), [first], "accepted: heartbeat right away, with the observation");
        assert!(a.is_member());
        assert!(ctx.os.joined, "membership is recorded on the host, so a reboot skips the join");
        a.on_message(&mut ctx, VIP, Msg::JoinResp(Ok(())));
        assert!(ctx.take().is_empty(), "a duplicate answer starts nothing twice");
        ctx.advance(&mut a, 35_000);
        let sent = ctx.take();
        assert_eq!(sent.iter().filter(|m| **m == heartbeat(15_000)).count(), 4, "{sent:?}");
        let metrics: Vec<_> = sent.iter().filter(|m| matches!(m, Msg::Metrics { .. })).collect();
        assert_eq!(metrics.len(), 2, "{sent:?}");
        assert!(!sent.iter().any(|m| matches!(m, Msg::JoinRequest { .. })), "{sent:?}");
        let Msg::Metrics { host, sample } = metrics[1] else { unreachable!() };
        assert_eq!(host.as_str(), "m1");
        assert_eq!((sample.at, sample.cpu_pct, sample.ram_pct, sample.temp_c), (35_000, 10.0, 20.0, 39.0));
        assert_eq!(sample.disk_pct["nvme0"], 5.0);
        assert_eq!((sample.port_rx_bps["eth0"], sample.port_tx_bps["eth0"]), (1.0, 2.0));
    }

    #[test]
    fn applied_network_is_kept() {
        let mut net = bootstrap_config(&profile(ProfileId::Pi5, 1));
        net.links[0].mtu = 9000;
        let mut ctx = FakeCtx::new(net.clone());
        HostAgent::new(Some("tok".into())).on_start(&mut ctx);
        assert_eq!(ctx.os.net, net);
    }

    /// The Pi 5's `sbc-transcode` config, with a static mgmt address.
    fn sbc_config() -> HostNetworkConfig {
        let mut cfg = bootstrap_config(&profile(ProfileId::Pi5, 1));
        cfg.attachments[0].ip = Some("10.0.0.7".parse().unwrap());
        cfg
    }

    fn apply(generation: u64, config: HostNetworkConfig) -> Msg {
        Msg::ApplyNetwork { generation, config, networks: vec![] }
    }

    fn status(generation: u64, result: NetworkResult) -> Msg {
        Msg::NetworkStatus { generation, result }
    }

    /// A member agent on a host running the bootstrap config, with nothing sent yet.
    fn member() -> (HostAgent, FakeCtx) {
        let mut ctx = FakeCtx::new(bootstrap_config(&profile(ProfileId::Pi5, 1)));
        let mut a = HostAgent::new(None);
        a.on_start(&mut ctx);
        ctx.take();
        (a, ctx)
    }

    #[test]
    fn a_confirmed_config_is_kept() {
        let (mut a, mut ctx) = member();
        a.on_message(&mut ctx, VIP, apply(1, sbc_config()));
        assert_eq!(ctx.os.net, sbc_config());
        assert_eq!(ctx.take(), [status(1, NetworkResult::Applied)], "reported at once, over the new config");
        assert_eq!(a.net_generation(), 1);
        ctx.advance(&mut a, 5000);
        assert_eq!(ctx.take_heartbeats(), [heartbeat_at_generation(0, 1)], "heartbeats carry the generation");
        assert_eq!(ctx.os.boot, bootstrap_config(&profile(ProfileId::Pi5, 1)), "on trial: a reboot goes back");
        a.on_message(&mut ctx, VIP, Msg::ConfirmNetwork { generation: 1 });
        assert_eq!(ctx.os.boot, sbc_config(), "confirmed: written for the next boot");
        ctx.advance(&mut a, 60_000);
        assert_eq!(ctx.os.net, sbc_config(), "confirmed: no revert");
        assert!(!ctx.take().iter().any(|m| matches!(m, Msg::NetworkStatus { .. })));
        assert_eq!(a.net_generation(), 1);
    }

    #[test]
    fn an_unconfirmed_config_is_reverted_after_30_s() {
        let (mut a, mut ctx) = member();
        let previous = ctx.os.net.clone();
        ctx.advance(&mut a, 1000);
        a.on_message(&mut ctx, VIP, apply(1, sbc_config()));
        ctx.take();
        ctx.advance(&mut a, 1000 + REVERT_MS - 1);
        assert_eq!(ctx.os.net, sbc_config(), "not yet");
        ctx.take();
        ctx.advance(&mut a, 1000 + REVERT_MS);
        assert_eq!(ctx.os.net, previous, "the previous config is back");
        assert_eq!(ctx.os.boot, previous, "and never was written for boot");
        assert_eq!(ctx.take(), [status(1, NetworkResult::Reverted)], "reported over the restored config");
        assert_eq!(a.net_generation(), 0);
        a.on_message(&mut ctx, VIP, Msg::ConfirmNetwork { generation: 1 });
        assert_eq!(ctx.os.net, previous, "a late confirmation changes nothing");
    }

    #[test]
    fn a_change_to_network_definitions_alone_is_tried_and_reverted_with_them() {
        let (mut a, mut ctx) = member();
        let before = model::seed_networks();
        a.on_message(&mut ctx, VIP, Msg::ApplyNetwork { generation: 1, config: sbc_config(), networks: before.clone() });
        a.on_message(&mut ctx, VIP, Msg::ConfirmNetwork { generation: 1 });
        ctx.take();
        let mut after = before.clone();
        after.iter_mut().find(|n| n.id.as_str() == "storage").unwrap().vlan = 21;
        ctx.advance(&mut a, 1000);
        a.on_message(&mut ctx, VIP, Msg::ApplyNetwork { generation: 2, config: sbc_config(), networks: after.clone() });
        assert_eq!((ctx.os.net.clone(), ctx.os.networks.clone()), (sbc_config(), after), "the same config, on the new definitions");
        assert_eq!(ctx.take(), [status(2, NetworkResult::Applied)]);
        ctx.advance(&mut a, 1000 + REVERT_MS);
        assert_eq!(ctx.os.networks, before, "unconfirmed: back on the definitions it ran with");
        assert_eq!(ctx.take_statuses(), [status(2, NetworkResult::Reverted)]);
        a.on_message(&mut ctx, VIP, Msg::ApplyNetwork { generation: 1, config: sbc_config(), networks: vec![] });
        assert_eq!(ctx.os.networks, before, "a stale push changes no definitions");
    }

    #[test]
    fn a_config_naming_a_missing_nic_fails_without_applying() {
        let (mut a, mut ctx) = member();
        let previous = ctx.os.net.clone();
        let mut cfg = sbc_config();
        cfg.links[0].kind = LinkKind::Single(PortName::from("sfp0"));
        a.on_message(&mut ctx, VIP, apply(1, cfg));
        assert_eq!(ctx.take(), [status(1, NetworkResult::Failed("no NIC sfp0".into()))]);
        assert_eq!((&ctx.os.net, a.net_generation()), (&previous, 0));
        ctx.advance(&mut a, 2 * REVERT_MS);
        assert_eq!(ctx.os.net, previous);
    }

    #[test]
    fn repeated_superseding_and_stale_pushes() {
        let (mut a, mut ctx) = member();
        let original = ctx.os.net.clone();
        a.on_message(&mut ctx, VIP, apply(1, sbc_config()));
        ctx.take();
        ctx.advance(&mut a, 10_000);
        // The leader did not hear back (or is new) and pushes again: same answer, same timer.
        a.on_message(&mut ctx, VIP, apply(1, sbc_config()));
        assert_eq!(ctx.take_statuses(), [status(1, NetworkResult::Applied)]);
        // A newer generation while still on trial: the fallback stays the last confirmed config.
        let mut next = sbc_config();
        next.links[0].mtu = 1400;
        a.on_message(&mut ctx, VIP, apply(2, next.clone()));
        assert_eq!((ctx.take_statuses(), &ctx.os.net), (vec![status(2, NetworkResult::Applied)], &next));
        a.on_message(&mut ctx, VIP, apply(1, sbc_config()));
        assert!(ctx.take_statuses().is_empty(), "an older generation is ignored");
        a.on_message(&mut ctx, VIP, Msg::ConfirmNetwork { generation: 1 });
        ctx.advance(&mut a, REVERT_MS);
        assert_eq!(ctx.os.net, next, "the first trial's timer no longer counts");
        ctx.take();
        ctx.advance(&mut a, 10_000 + REVERT_MS);
        assert_eq!((&ctx.os.net, a.net_generation()), (&original, 0), "never confirmed: back to the original");
        assert_eq!(ctx.take_statuses(), [status(2, NetworkResult::Reverted)]);
    }

    #[test]
    fn a_pushed_config_also_proves_the_join() {
        let mut ctx = FakeCtx::new(HostNetworkConfig::default());
        let mut a = HostAgent::new(Some("tok".into()));
        a.on_start(&mut ctx);
        ctx.take();
        // The JoinResp was lost, but only a member is told its network config.
        a.on_message(&mut ctx, VIP, apply(1, sbc_config()));
        assert!(a.is_member() && ctx.os.joined);
        assert_eq!(ctx.take(), [reporting(0, &ctx.os), status(1, NetworkResult::Applied)]);
        assert_eq!(ctx.os.net, sbc_config());
        ctx.advance(&mut a, 5000);
        assert_eq!(ctx.take_heartbeats(), [heartbeat_at_generation(0, 1)], "one heartbeat chain, no join retries");
    }

    #[test]
    fn a_restarted_agent_does_not_know_its_generation() {
        let (mut a, mut ctx) = member();
        a.on_message(&mut ctx, VIP, apply(3, sbc_config()));
        a.on_message(&mut ctx, VIP, Msg::ConfirmNetwork { generation: 3 });
        let mut restarted = HostAgent::new(None);
        restarted.on_start(&mut ctx);
        assert_eq!(restarted.net_generation(), 0, "the leader re-pushes what it wants");
        assert_eq!(ctx.os.net, sbc_config(), "the host keeps running what it had");
    }

    #[test]
    fn joined_agent_only_heartbeats() {
        let mut ctx = FakeCtx::new(HostNetworkConfig::default());
        let mut a = HostAgent::new(None);
        a.on_start(&mut ctx);
        assert_eq!(ctx.os.net, HostNetworkConfig::default(), "a member's network is not reset");
        assert_eq!(ctx.take(), [reporting(0, &ctx.os)]);
        ctx.advance(&mut a, 5000);
        assert_eq!(ctx.take(), [heartbeat(0)]);
    }

    #[test]
    fn observation_is_sent_on_start_on_change_and_on_request_only() {
        let mut ctx = FakeCtx::new(HostNetworkConfig::default());
        let mut a = HostAgent::new(None);
        a.on_start(&mut ctx);
        assert_eq!(ctx.take_heartbeats(), [reporting(0, &ctx.os)], "the first heartbeat after start carries it");
        ctx.advance(&mut a, 10_000);
        assert_eq!(ctx.take_heartbeats(), [heartbeat(0), heartbeat(0)], "unchanged: stamp only");

        // The cable moves: LLDP changes, and the next heartbeat says so with a new stamp.
        ctx.os.lldp = vec![neighbor("sw-2", "p7")];
        ctx.advance(&mut a, 15_000);
        assert_eq!(ctx.take_heartbeats(), [reporting(15_000, &ctx.os)]);
        ctx.advance(&mut a, 20_000);
        assert_eq!(ctx.take_heartbeats(), [heartbeat(15_000)]);

        // A NIC loses its link: the inventory changed, so that is reported too.
        ctx.os.inventory.nics[0].link_up = false;
        ctx.advance(&mut a, 25_000);
        assert_eq!(ctx.take_heartbeats(), [reporting(25_000, &ctx.os)]);

        // The leader lost it (new leader, lost message): asking gets an immediate full report.
        a.on_message(&mut ctx, VIP, Msg::ReportRequest);
        assert_eq!(ctx.take_heartbeats(), [reporting(25_000, &ctx.os)], "same stamp, full content");
        ctx.advance(&mut a, 30_000);
        assert_eq!(ctx.take_heartbeats(), [heartbeat(25_000)]);

        // A restarted agent has no memory of what it sent.
        let mut restarted = HostAgent::new(None);
        ctx.now = 40_000;
        restarted.on_start(&mut ctx);
        assert_eq!(ctx.take_heartbeats(), [reporting(40_000, &ctx.os)]);
    }

    fn spec(id: &str) -> InstanceSpec {
        let flow = FlowSpec { kind: FlowKind::Pod, to: FlowEnd::Router, network: NetworkId::from("mgmt"), min_mbps: 1.0, max_mbps: 10.0 };
        InstanceSpec { id: id.into(), cpu_m: 250, ram_mb: 256, usage: UsageProfile { base_pct: 20.0, noise_pct: 10.0, burst_every_s: 0 }, flows: vec![flow] }
    }

    #[test]
    fn workload_messages_reach_the_host_and_heartbeats_report_what_it_runs() {
        let (mut a, mut ctx) = member();
        a.on_message(&mut ctx, VIP, Msg::StartInstance(spec("pod/web/0")));
        a.on_message(&mut ctx, VIP, Msg::StartInstance(spec("vm/vm1")));
        a.on_message(&mut ctx, VIP, Msg::StopInstance("pod/web/0".into()));
        a.on_message(&mut ctx, VIP, Msg::CreateReplica { volume: "disk1".into(), size_gb: 40, source: None });
        a.on_message(&mut ctx, VIP, Msg::CreateReplica { volume: "disk2".into(), size_gb: 40, source: Some(HostId::from("m2")) });
        a.on_message(&mut ctx, VIP, Msg::DeleteReplica { volume: "disk1".into() });
        assert!(ctx.take().is_empty(), "nothing is answered: heartbeats tell");
        assert_eq!(ctx.os.instances, [spec("vm/vm1")]);
        ctx.advance(&mut a, 10_000);
        let sent = ctx.take();
        let Some(Msg::Heartbeat { instances, replicas, .. }) = sent.iter().find(|m| matches!(m, Msg::Heartbeat { .. })) else {
            panic!("no heartbeat: {sent:?}")
        };
        assert_eq!(instances, &["vm/vm1".to_string()]);
        assert_eq!(replicas, &[ReplicaReport { volume: "disk2".into(), disk: "nvme0".into(), pct: 0, copied_bytes: 0, copying: true }]);
        let Some(Msg::Metrics { sample, .. }) = sent.iter().find(|m| matches!(m, Msg::Metrics { .. })) else { panic!("no metrics") };
        assert_eq!(sample.instances, 1);
    }

    /// The incoming instance `id` takes over on the fake host.
    fn take_over(os: &mut FakeOs, id: &str) {
        let i = os.incoming.iter().position(|(s, _)| s.id == id).unwrap();
        let (spec, _) = os.incoming.remove(i);
        os.instances.push(spec);
    }

    fn done_reports(ctx: &mut FakeCtx) -> Vec<Msg> {
        ctx.take().into_iter().filter(|m| matches!(m, Msg::MigrationDone { .. })).collect()
    }

    /// `MigrateIn` of `id` from m2, given up at `deadline`.
    fn migrate_in(id: &str, deadline: SimTime) -> Msg {
        Msg::MigrateIn { spec: spec(id), from: HostId::from("m2"), deadline }
    }

    #[test]
    fn an_incoming_instance_is_reported_once_it_runs() {
        let (mut a, mut ctx) = member();
        a.on_message(&mut ctx, VIP, migrate_in("vm/vm1", 100_000));
        assert_eq!(ctx.os.incoming, [(spec("vm/vm1"), HostId::from("m2"))]);
        ctx.advance(&mut a, 3000);
        assert!(done_reports(&mut ctx).is_empty(), "still copying");
        take_over(&mut ctx.os, "vm/vm1");
        ctx.advance(&mut a, 4000);
        assert_eq!(done_reports(&mut ctx), [Msg::MigrationDone { instance: "vm/vm1".into() }]);
        ctx.advance(&mut a, 20_000);
        assert!(done_reports(&mut ctx).is_empty(), "reported once");
        // A repeat of the request for an instance that runs is answered again.
        a.on_message(&mut ctx, VIP, migrate_in("vm/vm1", 100_000));
        ctx.advance(&mut a, 21_000);
        assert_eq!(done_reports(&mut ctx), [Msg::MigrationDone { instance: "vm/vm1".into() }]);
        assert!(ctx.os.incoming.is_empty());
    }

    #[test]
    fn an_incoming_instance_still_copying_at_its_deadline_is_given_up() {
        let (mut a, mut ctx) = member();
        let deadline = 50_000;
        a.on_message(&mut ctx, VIP, migrate_in("vm/vm1", deadline));
        a.on_message(&mut ctx, VIP, migrate_in("vm/vm1", deadline));
        ctx.advance(&mut a, deadline - 1);
        assert_eq!(ctx.os.incoming.len(), 1, "a repeat changes nothing");
        ctx.advance(&mut a, deadline + MIGRATION_POLL_MS);
        assert!(ctx.os.incoming.is_empty() && done_reports(&mut ctx).is_empty(), "given up, unreported");
        // A request whose deadline has passed starts nothing.
        let t = ctx.now;
        a.on_message(&mut ctx, VIP, migrate_in("vm/vm1", t));
        assert!(ctx.os.incoming.is_empty());
        // One aborted by the leader is forgotten: a new attempt watches its own deadline.
        a.on_message(&mut ctx, VIP, migrate_in("vm/vm2", t + 1000));
        a.on_message(&mut ctx, VIP, Msg::AbortMigration("vm/vm2".into()));
        assert!(ctx.os.incoming.is_empty());
        ctx.advance(&mut a, ctx.now + 5000);
        let t = ctx.now;
        a.on_message(&mut ctx, VIP, migrate_in("vm/vm2", t + 60_000));
        ctx.advance(&mut a, t + 59_000);
        assert_eq!(ctx.os.migrations(), ["vm/vm2"]);
        assert_eq!(ctx.timers.len(), 1 + 2, "one poll pending, beside the heartbeat and metrics timers");
    }

    #[test]
    fn an_abort_leaves_an_instance_that_runs_alone() {
        let (mut a, mut ctx) = member();
        a.on_message(&mut ctx, VIP, migrate_in("vm/vm1", 100_000));
        take_over(&mut ctx.os, "vm/vm1");
        a.on_message(&mut ctx, VIP, Msg::AbortMigration("vm/vm1".into()));
        assert_eq!(ctx.os.instances(), ["vm/vm1"]);
    }

    #[test]
    fn an_unjoined_agent_runs_no_workloads() {
        let mut ctx = FakeCtx::new(HostNetworkConfig::default());
        let mut a = HostAgent::new(Some("tok".into()));
        a.on_start(&mut ctx);
        a.on_message(&mut ctx, VIP, Msg::StartInstance(spec("vm/vm1")));
        a.on_message(&mut ctx, VIP, Msg::CreateReplica { volume: "disk1".into(), size_gb: 40, source: None });
        assert!(ctx.os.instances.is_empty() && ctx.os.replicas.is_empty());
    }

    #[test]
    fn start_cp_replica_reaches_the_host_of_a_member_only() {
        let voters: BTreeSet<HostId> = [HostId::from("h1"), HostId::from("h2")].into();
        let mut ctx = FakeCtx::new(HostNetworkConfig::default());
        let mut a = HostAgent::new(Some("tok".into()));
        a.on_start(&mut ctx);
        a.on_message(&mut ctx, VIP, Msg::StartCpReplica { voters: voters.clone() });
        assert!(ctx.os.cp_spawned.is_empty(), "an unjoined host runs no replica");

        let (mut a, mut ctx) = member();
        a.on_message(&mut ctx, VIP, Msg::StartCpReplica { voters: voters.clone() });
        assert_eq!(ctx.os.cp_spawned, [voters]);
        assert!(ctx.take().is_empty());
    }

    #[test]
    fn an_unjoined_agent_ignores_report_requests() {
        let mut ctx = FakeCtx::new(HostNetworkConfig::default());
        let mut a = HostAgent::new(Some("tok".into()));
        a.on_start(&mut ctx);
        ctx.take();
        a.on_message(&mut ctx, VIP, Msg::ReportRequest);
        assert!(ctx.take().is_empty());
    }
}
