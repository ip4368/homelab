//! The runtime interface: what an [`Actor`] can see and do. The simulator
//! implements [`Ctx`]; control code only ever talks to this trait.

use crate::proto::Msg;
use model::{HostId, NetworkId};
use serde::{Deserialize, Serialize};

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

/// Host operating-system surface visible to an agent. Grows per task.
pub trait HostOs {
    fn host_id(&self) -> HostId;
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

/// `Send` so a whole simulation can be moved onto a server thread.
pub trait Actor: Send {
    fn on_start(&mut self, ctx: &mut dyn Ctx);
    fn on_message(&mut self, ctx: &mut dyn Ctx, from: Endpoint, msg: Msg);
    fn on_timer(&mut self, ctx: &mut dyn Ctx, token: u64);
}
