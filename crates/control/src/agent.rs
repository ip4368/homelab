//! The host agent: the daemon on every machine. An unjoined agent boots its
//! host on the bootstrap network config and asks the control plane (via the
//! VIP) to join with a token until it is accepted. A joined agent heartbeats
//! and reports metrics.

use crate::proto::{mgmt, MetricSample, Msg, Observed};
use crate::runtime::SimTime;
use crate::runtime::{Actor, Addr, Ctx, Endpoint, Svc};
use model::network::bootstrap_config;
use model::{profile, Role};
use std::collections::BTreeSet;

pub const JOIN_RETRY_MS: u64 = 5000;
pub const HEARTBEAT_MS: u64 = 5000;
pub const METRICS_MS: u64 = 10_000;
/// Synthetic temperature: idle plus a share of CPU load.
const IDLE_TEMP_C: f32 = 35.0;
const TEMP_C_PER_CPU_PCT: f32 = 0.4;

const VIP: Endpoint = Endpoint { addr: Addr::Vip, svc: Svc::Cp };

const JOIN: u64 = 0;
const HEARTBEAT: u64 = 1;
const METRICS: u64 = 2;

#[derive(Debug, Default)]
pub struct HostAgent {
    /// `Some` until the control plane accepts the join.
    token: Option<String>,
    /// As last told by the control plane.
    roles: BTreeSet<Role>,
    /// What the agent last observed about its host, and when that changed.
    observed: Option<(SimTime, Observed)>,
}

impl HostAgent {
    /// `token` is the join token for an unjoined host; `None` means the host
    /// already joined (e.g. it rebooted) and the agent just heartbeats.
    pub fn new(token: Option<String>) -> Self {
        Self { token, roles: BTreeSet::new(), observed: None }
    }

    pub fn roles(&self) -> &BTreeSet<Role> {
        &self.roles
    }

    /// Whether the agent runs as a cluster member (not trying to join).
    pub fn is_member(&self) -> bool {
        self.token.is_none()
    }

    fn send_join(&self, ctx: &mut dyn Ctx) {
        let (Some(token), Some(os)) = (&self.token, ctx.host()) else { return };
        let msg = Msg::JoinRequest { token: token.clone(), inventory: os.inventory() };
        ctx.send(VIP, mgmt(), msg);
    }

    /// Heartbeats the host's liveness. The observation (LLDP and inventory)
    /// rides along only when it changed since the last one, on the first
    /// heartbeat after start, or when `report` asks for it; every heartbeat
    /// carries the observation's stamp so the leader can tell if it has it.
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
        ctx.send(VIP, mgmt(), Msg::Heartbeat { host, lifecycle_seen: 0, observed_at, observed });
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
            Msg::Approved { roles } => {
                self.roles = roles;
                // Approval also proves the join went through, if its answer was lost.
                if self.token.is_some() {
                    self.start_member(ctx);
                } else {
                    // Confirm promptly: the control plane waits for a heartbeat to mark us Ready.
                    self.send_heartbeat(ctx, false);
                }
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
    use model::network::{HostNetworkConfig, LldpNeighbor};
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
        Msg::Heartbeat { host: HostId::from("m1"), lifecycle_seen: 0, observed_at: Some(at), observed: None }
    }

    /// A heartbeat carrying the observation, which is `os`'s.
    fn reporting(at: SimTime, os: &FakeOs) -> Msg {
        Msg::Heartbeat { host: HostId::from("m1"), lifecycle_seen: 0, observed_at: Some(at), observed: Some(observed(os)) }
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

    #[test]
    fn approval_records_roles_and_confirms_with_a_heartbeat() {
        let mut ctx = FakeCtx::new(HostNetworkConfig::default());
        let mut a = HostAgent::new(Some("tok".into()));
        a.on_start(&mut ctx);
        ctx.take();
        // The JoinResp was lost, but approval shows the join went through.
        let roles: BTreeSet<Role> = [Role::Transcode].into();
        a.on_message(&mut ctx, VIP, Msg::Approved { roles: roles.clone() });
        assert_eq!(a.roles(), &roles);
        assert!(a.is_member() && ctx.os.joined);
        assert_eq!(ctx.take(), [reporting(0, &ctx.os)]);
        a.on_message(&mut ctx, VIP, Msg::Approved { roles });
        assert_eq!(ctx.take(), [heartbeat(0)], "each approval is confirmed");
        ctx.advance(&mut a, 5000);
        assert_eq!(ctx.take(), [heartbeat(0)], "one heartbeat chain, no join retries");
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
