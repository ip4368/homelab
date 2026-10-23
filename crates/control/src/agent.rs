//! The host agent: the daemon on every machine. An unjoined agent boots its
//! host on the bootstrap network config and asks the control plane (via the
//! VIP) to join with a token until it is accepted. A joined agent heartbeats,
//! reports metrics, and applies the network configs the leader pushes, like
//! `netplan try`: a config that is not confirmed within [`REVERT_MS`] (because
//! it cut the host off from the control plane) is rolled back.

use crate::proto::{mgmt, MetricSample, Msg, Observed};
use crate::runtime::{Actor, Addr, Ctx, Endpoint, SimTime, Svc};
use crate::state::NetworkResult;
use model::network::{bootstrap_config, HostNetworkConfig};
use model::{profile, Inventory};

pub const JOIN_RETRY_MS: u64 = 5000;
pub const HEARTBEAT_MS: u64 = 5000;
pub const METRICS_MS: u64 = 10_000;
/// How long a newly applied network config waits for the leader's confirmation.
pub const REVERT_MS: u64 = 30_000;
/// Synthetic temperature: idle plus a share of CPU load.
const IDLE_TEMP_C: f32 = 35.0;
const TEMP_C_PER_CPU_PCT: f32 = 0.4;

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
    /// The last confirmed config and its generation.
    previous: HostNetworkConfig,
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
}

impl HostAgent {
    /// `token` is the join token for an unjoined host; `None` means the host
    /// already joined (e.g. it rebooted) and the agent just heartbeats.
    pub fn new(token: Option<String>) -> Self {
        Self { token, observed: None, net_generation: 0, trial: None }
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
        ctx.send(VIP, mgmt(), Msg::Heartbeat { host, lifecycle_seen: 0, observed_at, observed, net_generation });
    }

    fn send_metrics(&self, ctx: &mut dyn Ctx) {
        let now = ctx.now();
        let Some(os) = ctx.host() else { return };
        let (host, usage) = (os.host_id(), os.usage());
        let sample = MetricSample {
            at: now,
            cpu_pct: usage.cpu_pct,
            ram_pct: usage.ram_pct,
            temp_c: IDLE_TEMP_C + TEMP_C_PER_CPU_PCT * usage.cpu_pct,
            disk_pct: usage.disk_pct,
            port_rx_bps: usage.port_bps.iter().map(|(p, &(rx, _))| (p.clone(), rx)).collect(),
            port_tx_bps: usage.port_bps.into_iter().map(|(p, (_, tx))| (p, tx)).collect(),
            instances: 0,
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
    fn apply(&mut self, ctx: &mut dyn Ctx, generation: u64, config: HostNetworkConfig) {
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
        let (previous, previous_generation) = match self.trial.take() {
            Some(t) => (t.previous, t.previous_generation),
            None => (os.applied_network(), self.net_generation),
        };
        os.apply_network(config);
        self.trial = Some(Trial { generation, since: now, previous, previous_generation });
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
            os.apply_network(t.previous);
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
            os.apply_network(bootstrap_config(&profile(inv.profile, inv.variant)));
        }
        self.send_join(ctx);
        ctx.set_timer(JOIN_RETRY_MS, JOIN);
    }

    fn on_message(&mut self, ctx: &mut dyn Ctx, _from: Endpoint, msg: Msg) {
        match msg {
            Msg::JoinResp(Ok(())) if self.token.is_some() => self.start_member(ctx),
            Msg::ApplyNetwork { generation, config } => {
                // Only members are told their config: this proves the join, if its answer was lost.
                if self.token.is_some() {
                    self.start_member(ctx);
                }
                self.apply(ctx, generation, config);
            }
            Msg::ConfirmNetwork { generation } => {
                self.trial.take_if(|t| t.generation == generation);
            }
            Msg::ReportRequest if self.token.is_none() => self.send_heartbeat(ctx, true),
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

    struct FakeOs {
        net: HostNetworkConfig,
        joined: bool,
        lldp: Vec<LldpNeighbor>,
        inventory: Inventory,
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
        fn apply_network(&mut self, cfg: HostNetworkConfig) {
            self.net = cfg;
        }
        fn applied_network(&self) -> HostNetworkConfig {
            self.net.clone()
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
            let os = FakeOs { net, joined: false, lldp: vec![neighbor("sw-1", "p1")], inventory: Inventory::from_profile(ProfileId::Pi5, 1) };
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
        LldpNeighbor { local_port: PortName::from("eth0"), switch: SwitchId::from(switch), switch_port: PortName::from(port) }
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
        Msg::Heartbeat { host: HostId::from("m1"), lifecycle_seen: 0, observed_at: Some(at), observed: None, net_generation: generation }
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
        Msg::ApplyNetwork { generation, config }
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
        a.on_message(&mut ctx, VIP, Msg::ConfirmNetwork { generation: 1 });
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
        assert_eq!(ctx.take(), [status(1, NetworkResult::Reverted)], "reported over the restored config");
        assert_eq!(a.net_generation(), 0);
        a.on_message(&mut ctx, VIP, Msg::ConfirmNetwork { generation: 1 });
        assert_eq!(ctx.os.net, previous, "a late confirmation changes nothing");
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
    fn an_unjoined_agent_ignores_report_requests() {
        let mut ctx = FakeCtx::new(HostNetworkConfig::default());
        let mut a = HostAgent::new(Some("tok".into()));
        a.on_start(&mut ctx);
        ctx.take();
        a.on_message(&mut ctx, VIP, Msg::ReportRequest);
        assert!(ctx.take().is_empty());
    }
}
