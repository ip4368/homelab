//! The runtime interface: what an [`Actor`] can see and do. The simulator
//! implements [`Ctx`]; control code only ever talks to this trait.

use crate::proto::{LogCmd, Msg};
use crate::raft::{Durable, DurableOp};
use model::network::HostNetworkConfig;
use model::{HostId, Inventory, NetworkId};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::BTreeMap;

/// Simulated time in milliseconds since the start of the run.
pub type SimTime = u64;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Addr {
    Host(HostId),
    /// The virtual IP held by the current control-plane leader.
    Vip,
    Client,
    Router,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Svc {
    Agent,
    Cp,
    Client,
    Router,
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
    fn apply_network(&mut self, cfg: HostNetworkConfig);
    /// The network config the OS is running; empty if none was ever applied.
    fn applied_network(&self) -> HostNetworkConfig;
    fn usage(&mut self) -> HostUsage;
    /// The CP replica state on this host's disk, if a replica ever ran here.
    fn load_cp(&self) -> Option<Durable<LogCmd>>;
    /// Writes a replica's initial state to disk, replacing whatever was there.
    fn store_cp(&mut self, durable: Durable<LogCmd>);
    /// Applies one Raft persist op to the stored state (no-op if none is stored).
    fn persist_cp(&mut self, op: DurableOp<LogCmd>);
}

pub trait Ctx {
    fn now(&self) -> SimTime;
    fn me(&self) -> Endpoint;
    fn send(&mut self, to: Endpoint, net: NetworkId, msg: Msg);
    /// Fire `on_timer(token)` on this actor `after_ms` of sim-time from now.
    fn set_timer(&mut self, after_ms: u64, token: u64);
    fn rand_u64(&mut self) -> u64;
    /// Claim the VIP for this endpoint (control-plane leader only).
    fn claim_vip(&mut self);
    fn host(&mut self) -> Option<&mut dyn HostOs>;
}

/// `Send` so a whole simulation can be moved onto a server thread; `Any` so
/// the simulator can reach a concrete actor (e.g. the UI client) by type.
pub trait Actor: Any + Send {
    fn on_start(&mut self, ctx: &mut dyn Ctx);
    fn on_message(&mut self, ctx: &mut dyn Ctx, from: Endpoint, msg: Msg);
    fn on_timer(&mut self, ctx: &mut dyn Ctx, token: u64);
}
