//! The runtime interface: what an [`Actor`] can see and do. The host
//! runtime (`node`) implements [`Ctx`]; control code only ever talks to this trait.

use crate::proto::{KubeAdmin, KubeSpec, KubeStatus, LogCmd, Msg};
use crate::raft::{Durable, DurableOp};
use model::network::{HostNetworkConfig, LldpNeighbor, ObservedSwitchPort};
use model::{HostId, Inventory, NetworkId, SwitchId};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};

/// Time in milliseconds: Unix wall-clock time on a real host.
pub type SimTime = u64;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Addr {
    Host(HostId),
    /// The virtual IP held by the current control-plane leader.
    Vip,
    Client,
    /// A short-lived admin client (e.g. `homelabd token`), told apart from
    /// other clients by a random id so replies reach the right one.
    Admin(u64),
    Router,
    /// A switch's management interface, which sits on the switch itself on
    /// the mgmt VLAN.
    Switch(SwitchId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Svc {
    Agent,
    Cp,
    Client,
    Router,
    Switch,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Endpoint {
    pub addr: Addr,
    pub svc: Svc,
}

impl Endpoint {
    pub fn new(addr: Addr, svc: Svc) -> Self {
        Self { addr, svc }
    }
}

/// Resource usage as the host OS measures it right now.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HostUsage {
    pub cpu_pct: f32,
    pub ram_pct: f32,
    /// The hottest temperature sensor; `None` if the host has none.
    pub temp_c: Option<f32>,
    /// Per disk name; failed disks are absent.
    pub disk_pct: BTreeMap<String, f32>,
    /// Per port name: (rx, tx) bits per second.
    pub port_bps: BTreeMap<String, (f64, f64)>,
}

/// Host operating-system surface visible to the actors running on a host
/// (its agent and CP replica). Grows per task.
pub trait HostOs {
    fn host_id(&self) -> HostId;
    /// The hardware as discovered now: catalogue profile, NIC link state, failed disks.
    fn inventory(&self) -> Inventory;
    /// What the host's LLDP daemon hears now: the switch port on the far end
    /// of every cable whose link is up, in NIC order.
    fn lldp(&self) -> Vec<LldpNeighbor>;
    /// Runs `cfg`, with `networks` as the definitions (VLAN, subnet, MTU)
    /// of the networks it attaches, on top of those it knew. A change to
    /// either is a change to apply.
    fn apply_network(&mut self, cfg: HostNetworkConfig, networks: Vec<model::LogicalNetwork>);
    /// The network config the OS is running; empty if none was ever applied.
    fn applied_network(&self) -> HostNetworkConfig;
    /// The network definitions the running config was applied with.
    fn applied_networks(&self) -> Vec<model::LogicalNetwork> {
        vec![]
    }
    /// Writes the running network config as the one the host boots with
    /// (until then, a reboot brings back the previous one).
    fn persist_network(&mut self);
    fn usage(&mut self) -> HostUsage;
    /// The CP replica state on this host's disk, if a replica ever ran here.
    fn load_cp(&self) -> Option<Durable<LogCmd>>;
    /// Writes a replica's initial state to disk, replacing whatever was there.
    fn store_cp(&mut self, durable: Durable<LogCmd>);
    /// Applies one Raft persist op to the stored state (no-op if none is stored).
    fn persist_cp(&mut self, op: DurableOp<LogCmd>);
    /// Records on disk that this host joined the cluster, so its agent boots
    /// as a member from now on instead of joining with a token.
    fn mark_joined(&mut self);
    /// Starts a CP replica on this host with empty Raft state that knows
    /// `voters`, unless the host already holds CP state (its replica runs, or
    /// resumes from disk on boot).
    fn spawn_cp_replica(&mut self, voters: BTreeSet<HostId>);
    /// Brings the host's Kubernetes in line with `spec`, in the background:
    /// installs it, reinstalls it as the other role, labels the node and
    /// gives Longhorn its disks. A repeat of the spec in hand changes nothing.
    fn apply_kube(&mut self, _spec: KubeSpec) {}
    /// Does `admin` for the cluster with this host's K3s server, in the
    /// background; nothing on a host that runs none.
    fn kube_admin(&mut self, _admin: KubeAdmin) {}
    /// How the host's Kubernetes stands now; `None` if this host has none to report.
    fn kube_status(&self) -> Option<KubeStatus> {
        None
    }
    /// An admin kubeconfig for the cluster's Kubernetes, pointed at the VIP;
    /// `None` unless this host runs a K3s server that wrote one.
    fn kubeconfig(&self) -> Option<String> {
        None
    }
}

/// A switch's own configuration, as its management interface reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwitchConfig {
    pub model: String,
    /// Every port, in port-name order.
    pub ports: Vec<ObservedSwitchPort>,
    pub mlag_peer: Option<SwitchId>,
}

/// What the actor running on a switch can see of it (its `Svc::Switch`).
pub trait SwitchOs {
    fn switch_id(&self) -> SwitchId;
    fn config(&self) -> SwitchConfig;
}

pub trait Ctx {
    fn now(&self) -> SimTime;
    /// Milliseconds on a clock that never steps, for Raft's election and
    /// lease timing. On a real host `now` is wall time, which chrony may step
    /// backwards; a test's clock is already monotonic, hence the default.
    fn monotonic_ms(&self) -> SimTime {
        self.now()
    }
    fn me(&self) -> Endpoint;
    fn send(&mut self, to: Endpoint, net: NetworkId, msg: Msg);
    /// Fire `on_timer(token)` on this actor `after_ms` from now.
    fn set_timer(&mut self, after_ms: u64, token: u64);
    fn rand_u64(&mut self) -> u64;
    /// Drops any connection to `to`, so the next message to it dials afresh:
    /// one to the VIP may still lead to a host that no longer holds it.
    fn redial(&mut self, _to: &Addr) {}
    /// Claim the VIP for this endpoint (control-plane leader only).
    fn claim_vip(&mut self);
    fn host(&mut self) -> Option<&mut dyn HostOs>;
    /// The switch this actor runs on, if it is a switch actor.
    fn switch(&mut self) -> Option<&mut dyn SwitchOs> {
        None
    }
}

/// `Send` so an actor can run on its runtime's thread; `Any` so the runtime
/// can reach a concrete actor (e.g. the UI client) by type.
pub trait Actor: Any + Send {
    fn on_start(&mut self, ctx: &mut dyn Ctx);
    fn on_message(&mut self, ctx: &mut dyn Ctx, from: Endpoint, msg: Msg);
    fn on_timer(&mut self, ctx: &mut dyn Ctx, token: u64);
}
