//! The host agent: the daemon on every machine. An unjoined agent boots its
//! host on the bootstrap network config and asks the control plane (via the
//! VIP) to join with a token until it is accepted. A joined agent heartbeats,
//! reports metrics, and applies the network configs the leader pushes, like
//! `netplan try`: a config that is not confirmed within [`REVERT_MS`] (because
//! it cut the host off from the control plane) is rolled back, and only a
//! confirmed one is written for the next boot. A member also runs what the
//! leader asks of Kubernetes (K3s), which runs the workloads.

use crate::proto::{mgmt, KubeHealth, KubeStatus, MetricSample, Msg, Observed};
use crate::runtime::{Actor, Addr, Ctx, Endpoint, SimTime, Svc};
use crate::state::NetworkResult;
use model::network::{bootstrap_config, HostNetworkConfig, LogicalNetwork};
use model::{profile, HostId, Inventory};

pub const JOIN_RETRY_MS: u64 = 5000;
pub const HEARTBEAT_MS: u64 = 5000;
pub const METRICS_MS: u64 = 10_000;
/// How long a newly applied network config waits for the leader's confirmation.
pub const REVERT_MS: u64 = 30_000;
/// How often a member reports its Kubernetes status even when unchanged.
pub const KUBE_REPORT_MS: u64 = 30_000;
/// How often a K3s server reports its API's health even when unchanged.
pub const KUBE_HEALTH_REPORT_MS: u64 = 10_000;

const VIP: Endpoint = Endpoint { addr: Addr::Vip, svc: Svc::Cp };

const JOIN: u64 = 0;
const HEARTBEAT: u64 = 1;
const METRICS: u64 = 2;
const REVERT: u64 = 3;

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
    /// The Kubernetes status last reported, and when.
    kube_reported: Option<(KubeStatus, SimTime)>,
    /// The K3s API health last reported, and when.
    health_reported: Option<(KubeHealth, SimTime)>,
    /// The repair attempt last carried out, if any.
    last_repair: Option<SimTime>,
}

impl HostAgent {
    /// `token` is the join token for an unjoined host; `None` means the host
    /// already joined (e.g. it rebooted) and the agent just heartbeats.
    pub fn new(token: Option<String>) -> Self {
        Self { token, observed: None, net_generation: 0, trial: None, kube_reported: None, health_reported: None, last_repair: None }
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
        let current = ctx.host().map(|os| Observed { lldp: os.lldp(), inventory: os.inventory() });
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
        let hb = Msg::Heartbeat { host: host.clone(), lifecycle_seen: 0, observed_at, observed, net_generation };
        ctx.send(VIP, mgmt(), hb);
        self.report_kube(ctx, host.clone(), now);
        self.report_health(ctx, host, now);
    }

    /// Reports the host's Kubernetes status if it changed since the last
    /// report, or every [`KUBE_REPORT_MS`] regardless (a report may be lost,
    /// and a new leader knows nothing).
    fn report_kube(&mut self, ctx: &mut dyn Ctx, host: HostId, now: SimTime) {
        let Some(status) = ctx.host().and_then(|os| os.kube_status()) else { return };
        let due = self.kube_reported.as_ref().is_none_or(|(last, at)| *last != status || now >= at + KUBE_REPORT_MS);
        if due {
            self.kube_reported = Some((status.clone(), now));
            ctx.send(VIP, mgmt(), Msg::KubeStatus { host, status });
        }
    }

    /// Reports the host's K3s API health, if it runs a server, when it
    /// changes or every [`KUBE_HEALTH_REPORT_MS`] regardless.
    fn report_health(&mut self, ctx: &mut dyn Ctx, host: HostId, now: SimTime) {
        let Some(health) = ctx.host().and_then(|os| os.kube_health()) else { return };
        let due = self.health_reported.as_ref().is_none_or(|(last, at)| *last != health || now >= at + KUBE_HEALTH_REPORT_MS);
        if due {
            self.health_reported = Some((health.clone(), now));
            ctx.send(VIP, mgmt(), Msg::KubeHealth { host, health });
        }
    }

    fn send_metrics(&self, ctx: &mut dyn Ctx) {
        let now = ctx.now();
        let Some(os) = ctx.host() else { return };
        let (host, usage) = (os.host_id(), os.usage());
        let sample = MetricSample {
            at: now,
            cpu_pct: usage.cpu_pct,
            ram_pct: usage.ram_pct,
            temp_c: usage.temp_c,
            disk_pct: usage.disk_pct,
            port_rx_bps: usage.port_bps.iter().map(|(p, &(rx, _))| (p.clone(), rx)).collect(),
            port_tx_bps: usage.port_bps.into_iter().map(|(p, (_, tx))| (p, tx)).collect(),
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
            Msg::StartCpReplica { voters } if self.token.is_none() => {
                if let Some(os) = ctx.host() {
                    os.spawn_cp_replica(voters);
                }
            }
            Msg::Kube(spec) if self.token.is_none() => {
                if let Some(os) = ctx.host() {
                    os.apply_kube(spec);
                }
            }
            Msg::KubeAdmin(admin) if self.token.is_none() => {
                if let Some(os) = ctx.host() {
                    os.kube_admin(admin);
                }
            }
            // Each recorded attempt runs once, however often leaders send it.
            Msg::KubeRepair { attempt } if self.token.is_none() && self.last_repair != Some(attempt) => {
                self.last_repair = Some(attempt);
                if let Some(os) = ctx.host() {
                    os.restart_kube();
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
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::LogCmd;
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
        /// The voters of every CP replica started.
        cp_spawned: Vec<BTreeSet<HostId>>,
        health: Option<KubeHealth>,
        restarts: u32,
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
                temp_c: Some(39.0),
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
        fn spawn_cp_replica(&mut self, voters: BTreeSet<HostId>) {
            self.cp_spawned.push(voters);
        }
        fn kube_health(&self) -> Option<KubeHealth> {
            self.health.clone()
        }
        fn restart_kube(&mut self) {
            self.restarts += 1;
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
                cp_spawned: vec![],
                health: None,
                restarts: 0,
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
        assert_eq!((sample.at, sample.cpu_pct, sample.ram_pct, sample.temp_c), (35_000, 10.0, 20.0, Some(39.0)));
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

    #[test]
    fn server_health_is_reported_on_change_and_repair_runs_once_per_attempt() {
        let (mut a, mut ctx) = member();
        let health = |ready: bool| Msg::KubeHealth { host: HostId::from("m1"), health: KubeHealth { api_ready: ready } };
        let takes = |ctx: &mut FakeCtx| ctx.take().into_iter().filter(|m| matches!(m, Msg::KubeHealth { .. })).collect::<Vec<_>>();

        ctx.os.health = Some(KubeHealth { api_ready: true });
        ctx.advance(&mut a, HEARTBEAT_MS);
        assert_eq!(takes(&mut ctx), [health(true)], "first report goes at once");
        ctx.advance(&mut a, HEARTBEAT_MS + KUBE_HEALTH_REPORT_MS - 1);
        assert!(takes(&mut ctx).is_empty(), "unchanged and not due");
        ctx.advance(&mut a, HEARTBEAT_MS + KUBE_HEALTH_REPORT_MS);
        assert_eq!(takes(&mut ctx), [health(true)], "re-sent on the interval");
        ctx.os.health = Some(KubeHealth { api_ready: false });
        ctx.advance(&mut a, 2 * HEARTBEAT_MS + KUBE_HEALTH_REPORT_MS);
        assert_eq!(takes(&mut ctx), [health(false)], "a change goes at the next heartbeat");

        a.on_message(&mut ctx, VIP, Msg::KubeRepair { attempt: 7 });
        a.on_message(&mut ctx, VIP, Msg::KubeRepair { attempt: 7 });
        assert_eq!(ctx.os.restarts, 1, "a repeated attempt restarts once");
        a.on_message(&mut ctx, VIP, Msg::KubeRepair { attempt: 8 });
        assert_eq!(ctx.os.restarts, 2);
    }
}
