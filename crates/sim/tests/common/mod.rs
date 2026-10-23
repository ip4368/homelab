//! Shared fixture: one switch, three MS-01 hosts each running a CP replica
//! and a heartbeat-only agent stub, and the UI client on the same switch.
//! Spare switch ports take hosts booted later with [`boot_host`].

use control::agent::HostAgent;
use control::client::{ClientActor, CLIENT};
use control::proto::{mgmt, ApiOp, ApiResult, LogCmd, Msg};
use control::raft::{Durable, Entry, EntryPayload};
use control::replica::CpReplica;
use control::runtime::{Actor, Addr, Ctx, Endpoint, Svc};
use control::state::{Command, CpMode, Lifecycle};
use model::{profile, seed_networks, HostId, Inventory, PortName, ProfileId, Role, SwitchId};
use sim::engine::Engine;
use sim::fabric::Fabric;
use sim::topology::{Node, PortMode, Switch, SwitchPortCfg};
use sim::world::{HostHw, World};
use std::collections::BTreeSet;

pub const HOSTS: [&str; 3] = ["h1", "h2", "h3"];
pub const SWITCH: &str = "sw-1";
pub const SWITCH_PORTS: usize = 8;
pub const HEARTBEAT_MS: u64 = 5000;

pub fn hid(s: &str) -> HostId {
    HostId::from(s)
}

pub fn cp(h: &HostId) -> Endpoint {
    Endpoint::new(Addr::Host(h.clone()), Svc::Cp)
}

pub fn agent(h: &HostId) -> Endpoint {
    Endpoint::new(Addr::Host(h.clone()), Svc::Agent)
}

/// Sends `Heartbeat` to the VIP every [`HEARTBEAT_MS`], nothing else.
pub struct HeartbeatStub {
    pub host: HostId,
}

impl Actor for HeartbeatStub {
    fn on_start(&mut self, ctx: &mut dyn Ctx) {
        ctx.set_timer(HEARTBEAT_MS, 0);
    }
    fn on_message(&mut self, _: &mut dyn Ctx, _: Endpoint, _: Msg) {}
    fn on_timer(&mut self, ctx: &mut dyn Ctx, _: u64) {
        let hb = Msg::Heartbeat { host: self.host.clone(), lifecycle_seen: 0, observed_at: None, observed: None, net_generation: 0 };
        ctx.send(Endpoint::new(Addr::Vip, Svc::Cp), mgmt(), hb);
        ctx.set_timer(HEARTBEAT_MS, 0);
    }
}

fn world() -> World {
    let mut w = World::new();
    let access = SwitchPortCfg { mode: PortMode::Access(1), mtu: 9216, lacp_group: None, admin_up: true };
    let ports = (1..=SWITCH_PORTS).map(|i| (PortName::new(format!("p{i}")), (2_500, access.clone()))).collect();
    let sw = SwitchId::from(SWITCH);
    w.topology.add_switch(Switch { id: sw.clone(), model: "test-switch".into(), ports, mlag_peer: None, up: true });
    for (i, h) in HOSTS.iter().enumerate() {
        let port = Node::SwitchPort(sw.clone(), PortName::new(format!("p{}", i + 1)));
        w.topology.connect(Node::HostPort(hid(h), PortName::from("eth0")), port);
        w.hosts.insert(hid(h), HostHw::new(ProfileId::Ms01, 0));
    }
    w.topology.connect(Node::Client, Node::SwitchPort(sw, PortName::from("p4")));
    w
}

/// The installed cluster every replica starts from: the seed networks, the
/// three hosts already `Ready` with the control-plane role, and them as CP voters.
pub fn seeded_durable() -> Durable<LogCmd> {
    let voters: BTreeSet<HostId> = HOSTS.iter().map(|h| hid(h)).collect();
    let mut cmds: Vec<Command> = seed_networks().into_iter().map(|network| Command::UpsertNetwork { network }).collect();
    for h in HOSTS {
        let id = hid(h);
        cmds.push(Command::AddPendingHost {
            id: id.clone(),
            profile: profile(ProfileId::Ms01, 0),
            inventory: Inventory::from_profile(ProfileId::Ms01, 0),
            location: "rack-0".into(),
        });
        cmds.push(Command::ApproveHost { id: id.clone(), roles: [Role::ControlPlane].into() });
        cmds.push(Command::SetLifecycle { id: id.clone(), lifecycle: Lifecycle::Provisioning });
        cmds.push(Command::SetLifecycle { id, lifecycle: Lifecycle::Ready });
    }
    cmds.push(Command::SetCp { voters: voters.clone(), mode: CpMode::Suggest });
    let mut d = Durable::new(voters);
    d.term = 1;
    d.log = cmds
        .into_iter()
        .enumerate()
        .map(|(i, cmd)| Entry { term: 1, index: i as u64 + 1, payload: EntryPayload::Normal(LogCmd { at: 0, cmd }) })
        .collect();
    d
}

/// The fixture, started at t = 0.
pub fn engine(seed: u64) -> Engine {
    let mut e = Engine::new(seed);
    e.set_deliver(Box::new(Fabric::new(world(), seed)));
    let durable = seeded_durable();
    for h in HOSTS {
        let id = hid(h);
        e.spawn(cp(&id), Box::new(CpReplica::new(id.clone(), Some(durable.clone()), durable.voters.clone())));
        e.spawn(agent(&id), Box::new(HeartbeatStub { host: id }));
    }
    e.spawn(CLIENT, Box::new(ClientActor::new()));
    e
}

/// Hosts whose replica currently believes it is leader.
pub fn leaders(e: &Engine) -> Vec<HostId> {
    HOSTS.iter().map(|h| hid(h)).filter(|h| e.actor::<CpReplica>(&cp(h)).is_some_and(|r| r.is_leader())).collect()
}

/// The single leader that every host's switch resolves the VIP to.
pub fn vip_leader(e: &Engine) -> Option<HostId> {
    let [l] = leaders(e).try_into().ok()?;
    HOSTS.iter().all(|h| e.fabric().resolve_vip(&hid(h)).as_ref() == Some(&l)).then_some(l)
}

/// Runs until a leader holds the VIP everywhere; panics after `within_ms`.
pub fn elect(e: &mut Engine, within_ms: u64) -> HostId {
    let deadline = e.now() + within_ms;
    assert!(e.run_until(deadline, |e| vip_leader(e).is_some()), "no leader with the VIP by t={deadline}");
    vip_leader(e).unwrap()
}

/// Submits `op` through the client and returns its `Ok` value; panics on
/// any other answer, or none within 3 s.
pub fn api_ok(e: &mut Engine, op: ApiOp) -> serde_json::Value {
    let req = e.api(op);
    e.run_for(3000);
    match e.take_api_results().as_slice() {
        [(r, ApiResult::Ok(v))] if *r == req => v.clone(),
        other => panic!("expected Ok for request {req}, got {other:?}"),
    }
}

/// A fresh join token from the leader.
pub fn join_token(e: &mut Engine) -> String {
    match api_ok(e, ApiOp::CreateJoinToken) {
        serde_json::Value::String(t) => t,
        other => panic!("expected a token string, got {other}"),
    }
}

/// Plugs a new host's `eth0` into the first free switch port and boots its
/// agent with `token`, as a technician would.
pub fn boot_host(e: &mut Engine, id: &str, p: ProfileId, variant: u8, token: Option<String>) -> HostId {
    let host = hid(id);
    let w = e.deliver_mut::<Fabric>().unwrap().world_mut();
    let free = (1..=SWITCH_PORTS)
        .map(|i| Node::SwitchPort(SwitchId::from(SWITCH), PortName::new(format!("p{i}"))))
        .find(|port| w.topology.peer(port).is_none())
        .expect("a free switch port");
    w.topology.connect(Node::HostPort(host.clone(), PortName::from("eth0")), free);
    w.hosts.insert(host.clone(), HostHw::new(p, variant));
    e.spawn(agent(&host), Box::new(HostAgent::new(token)));
    host
}
