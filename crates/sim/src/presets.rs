//! Seed fleets: a cabled topology plus the control-plane state a cluster
//! would have right after installation, so a preset starts out `Ready`, each
//! host running its rule's network template on technician-configured ports.
//!
//! Topology: `core-1` and `core-2` are an MLAG pair of 64-port 10G SFP+
//! switches joined by a 40G link. Access switches `access-1..N` (48× 2.5G,
//! 4× 10G uplinks) each uplink once to each core. The router and the UI
//! client hang off `core-1`. Hosts are cabled with [`cable_host`], which the
//! join scenario reuses.

use crate::engine::Engine;
use crate::fabric::Fabric;
use crate::switch_actor::SwitchActor;
use crate::topology::{Node, PortMode, Switch, SwitchPortCfg};
use crate::world::{HostHw, World};
use control::agent::HostAgent;
use control::client::{ClientActor, CLIENT};
use control::controllers::join::allocate_ips;
use control::proto::LogCmd;
use control::raft::{Durable, Entry, EntryPayload};
use control::replica::CpReplica;
use control::runtime::{Actor, Addr, Endpoint, Svc};
use control::state::{Command, CpMode, Lifecycle, NetworkResult};
use model::{eligible, render, seed_networks, seed_templates, HostId, PortName, ProfileId, Role, SwitchId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use ts_rs::TS;

pub const CORE_1: &str = "core-1";
pub const CORE_2: &str = "core-2";
const CORE_PORTS: u32 = 64;
const CORE_PORT_MBPS: u32 = 10_000;
/// The 40G inter-core link's port, on both cores.
pub const INTER_CORE_PORT: &str = "qsfp1";
const INTER_CORE_MBPS: u32 = 40_000;
pub const ROUTER_PORT: &str = "p63";
pub const CLIENT_PORT: &str = "p64";
/// Core ports below the router's; MS-01s take them from the bottom, access uplinks from the top.
const CORE_HOST_PORTS: u32 = 62;

const ACCESS_PORTS: u32 = 48;
const ACCESS_PORT_MBPS: u32 = 2_500;
/// Ports `p1..=p46` take hosts; the rest are spare.
pub const ACCESS_HOST_PORTS: u32 = 46;
/// `sfp1` goes to core-1, `sfp2` to core-2; `sfp3` and `sfp4` are spare.
const ACCESS_UPLINKS: u32 = 4;
const UPLINK_MBPS: u32 = 10_000;

const SWITCH_MTU: u16 = 9216;
/// The VLAN untagged mgmt traffic rides on.
const MGMT_VLAN: u16 = 1;
const ROUTER_VLANS: [u16; 3] = [1, 100, 110];
/// What a mini PC's port carries: mgmt untagged, storage tagged.
const MINIPC_VLANS: [u16; 2] = [1, 20];
/// What an MS-01's SFP+ bond carries tagged: storage, migration and the VM networks.
const MS01_BOND_VLANS: [u16; 4] = [20, 30, 100, 110];
/// MS-01s sit next to the cores.
const CORE_RACK: &str = "rack-0";

/// How many hosts of each profile a fleet has.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct FleetSpec {
    pub ms01: u32,
    pub minipc: u32,
    pub pi5: u32,
    pub rk3588: u32,
}

impl FleetSpec {
    pub fn total(&self) -> u32 {
        self.ms01 + self.minipc + self.pi5 + self.rk3588
    }

    /// Profiles in cabling order, with their counts.
    fn counts(&self) -> [(ProfileId, u32); 4] {
        [
            (ProfileId::Ms01, self.ms01),
            (ProfileId::MiniPc, self.minipc),
            (ProfileId::Pi5, self.pi5),
            (ProfileId::Rk3588, self.rk3588),
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum Preset {
    /// 60 hosts.
    Default,
    /// 500 hosts.
    Scale500,
}

impl Preset {
    pub fn spec(self) -> FleetSpec {
        match self {
            Preset::Default => FleetSpec { ms01: 12, minipc: 24, pi5: 12, rk3588: 12 },
            Preset::Scale500 => FleetSpec { ms01: 40, minipc: 200, pi5: 120, rk3588: 140 },
        }
    }

    /// Initial CP voters. The default fleet starts at 3 and grows to 5 later
    /// (Task 24); the scale fleet starts at 5.
    pub fn cp_voters(self) -> usize {
        match self {
            Preset::Default => 3,
            Preset::Scale500 => 5,
        }
    }
}

/// Actors to spawn, in order, to bring a fleet up.
pub type InitialActors = Vec<(Endpoint, Box<dyn Actor>)>;

impl Engine {
    pub fn load_preset(&mut self, p: Preset) {
        self.load_fleet(&p.spec(), p.cp_voters());
    }

    /// Replaces everything with a freshly built fleet ([`build_fleet`]) on a
    /// fabric seeded from the engine's seed.
    pub fn load_fleet(&mut self, spec: &FleetSpec, cp_voters: usize) {
        let (world, actors) = build_fleet(spec, cp_voters);
        self.reset(Box::new(Fabric::new(world, self.seed())));
        for (at, actor) in actors {
            self.spawn(at, actor);
        }
    }
}

/// The world for `spec` and the actors that run it: a CP replica on each of
/// the first `cp_voters` control-plane-eligible hosts (MS-01s come first),
/// all starting from the same [`seeded_durable`]; a member agent on every
/// host; a management interface on every switch; and the UI client. Every
/// host runs its seed rule's template, addressed in cabling order.
pub fn build_fleet(spec: &FleetSpec, cp_voters: usize) -> (World, InitialActors) {
    let mut w = World::new();
    add_cores(&mut w);
    for n in 1..=spec.total().div_ceil(ACCESS_HOST_PORTS).max(1) {
        add_access_switch(&mut w, n);
    }
    let templates: BTreeMap<String, _> = seed_templates().into_iter().map(|t| (t.id.clone(), t)).collect();
    let mut taken = BTreeSet::new();
    let mut hosts = vec![];
    for (profile, count) in spec.counts() {
        let template = &templates[seed_rule_template(profile)];
        for i in 0..count {
            let id = host_id(profile, i + 1);
            let ips = allocate_ips(&template.config, |n| w.networks.get(n), &mut taken);
            let hw = HostHw { applied_net: render(template, None, &ips), ..HostHw::new(profile, (i % 2) as u8) };
            w.hosts.insert(id.clone(), hw);
            let cabled = cable_host(&mut w, &id, profile).expect("the fleet's switches have room for it");
            hosts.push((id, cabled.location));
        }
    }
    let voters: BTreeSet<HostId> = hosts
        .iter()
        .map(|(id, _)| id)
        .filter(|id| eligible(&w.hosts[*id].profile, Role::ControlPlane).is_ok())
        .take(cp_voters)
        .cloned()
        .collect();
    let durable = seeded_durable(&w, &hosts, &voters);
    let mut actors: InitialActors = vec![];
    for v in &voters {
        let replica = CpReplica::new(v.clone(), Some(durable.clone()), voters.clone());
        actors.push((Endpoint::new(Addr::Host(v.clone()), Svc::Cp), Box::new(replica)));
    }
    for (id, _) in &hosts {
        actors.push((Endpoint::new(Addr::Host(id.clone()), Svc::Agent), Box::new(HostAgent::new(None))));
    }
    for id in w.topology.switches.keys() {
        actors.push((Endpoint::new(Addr::Switch(id.clone()), Svc::Switch), Box::new(SwitchActor)));
    }
    actors.push((CLIENT, Box::new(ClientActor::new())));
    (w, actors)
}

/// The Raft state of a freshly installed cluster: one term-1 log entry per
/// command, all uncommitted until the first leader's no-op commits them.
/// The log sets the CP voters, adds the seed networks and templates and the
/// seed auto-approve rules (each naming its template), and adds every host
/// at `location`, approved with its rule's roles (plus `ControlPlane` for
/// voters), provisioned on its rule's template with the addresses of the
/// config it runs, that config applied as generation 1, and `Ready`.
pub fn seeded_durable(w: &World, hosts: &[(HostId, String)], voters: &BTreeSet<HostId>) -> Durable<LogCmd> {
    let mut cmds = vec![Command::SetCp { voters: voters.clone(), mode: CpMode::Suggest }];
    cmds.extend(seed_networks().into_iter().map(|network| Command::UpsertNetwork { network }));
    cmds.extend(seed_templates().into_iter().map(|template| Command::UpsertTemplate { template }));
    for p in [ProfileId::Ms01, ProfileId::MiniPc, ProfileId::Pi5, ProfileId::Rk3588] {
        cmds.push(Command::UpsertRule {
            id: profile_prefix(p).into(),
            profile: p,
            roles: seed_rule_roles(p),
            template: Some(seed_rule_template(p).into()),
        });
    }
    for (id, location) in hosts {
        let hw = &w.hosts[id];
        let mut roles: BTreeSet<Role> =
            seed_rule_roles(hw.profile.id).into_iter().filter(|&r| eligible(&hw.profile, r).is_ok()).collect();
        if voters.contains(id) {
            roles.insert(Role::ControlPlane);
        }
        let ips = hw.applied_net.attachments.iter().filter_map(|a| Some((a.network.clone(), a.ip?))).collect();
        cmds.extend([
            Command::AddPendingHost {
                id: id.clone(),
                profile: hw.profile.clone(),
                inventory: w.inventory(id).expect("the host is in the world"),
                location: location.clone(),
            },
            Command::ApproveHost { id: id.clone(), roles },
            Command::ProvisionHost { id: id.clone(), template: Some(seed_rule_template(hw.profile.id).into()), ips },
            Command::SetDesiredNetwork { id: id.clone(), config: hw.applied_net.clone() },
            Command::RecordNetworkResult { id: id.clone(), generation: 1, result: NetworkResult::Applied },
            Command::SetLifecycle { id: id.clone(), lifecycle: Lifecycle::Ready },
        ]);
    }
    let mut d = Durable::new(voters.clone());
    d.term = 1;
    d.log = cmds
        .into_iter()
        .zip(1..)
        .map(|(cmd, index)| Entry { term: 1, index, payload: EntryPayload::Normal(LogCmd { at: 0, cmd }) })
        .collect();
    d
}

/// The seed auto-approve rule's roles for a profile.
pub fn seed_rule_roles(p: ProfileId) -> BTreeSet<Role> {
    match p {
        ProfileId::Ms01 => [Role::Storage, Role::Vm, Role::Compute].into(),
        ProfileId::MiniPc => [Role::Compute, Role::Transcode, Role::Storage].into(),
        ProfileId::Pi5 | ProfileId::Rk3588 => [Role::Transcode, Role::Compute].into(),
    }
}

/// The network template a profile's seed auto-approve rule applies.
pub fn seed_rule_template(p: ProfileId) -> &'static str {
    match p {
        ProfileId::Ms01 => "ms01-standard",
        ProfileId::MiniPc => "minipc-standard",
        // `sbc-transcode` is profiled Pi5 but deliberately shared by Rk3588 (same eth0-only NICs).
        ProfileId::Pi5 | ProfileId::Rk3588 => "sbc-transcode",
    }
}

/// Host ids are `<prefix>-<nn>`, e.g. `ms01-01`.
pub fn profile_prefix(p: ProfileId) -> &'static str {
    match p {
        ProfileId::Ms01 => "ms01",
        ProfileId::MiniPc => "minipc",
        ProfileId::Pi5 => "pi5",
        ProfileId::Rk3588 => "rk3588",
    }
}

pub fn host_id(p: ProfileId, n: u32) -> HostId {
    HostId::new(format!("{}-{n:02}", profile_prefix(p)))
}

/// The configuration of a port a host's bootstrap mgmt arrives on.
pub fn host_port_cfg() -> SwitchPortCfg {
    SwitchPortCfg { mode: PortMode::Access(MGMT_VLAN), mtu: SWITCH_MTU, lacp_group: None, admin_up: true }
}

/// How a technician configures the switch port `nic` of a new host lands
/// on, to suit the profile's seed template: an access port on the mgmt VLAN
/// for SBCs and every `eth0` but a mini PC's, a trunk with mgmt native and
/// storage tagged for a mini PC, and an LACP trunk (group named after the
/// host) carrying the tagged networks for an MS-01's SFP+ ports.
pub fn technician_port_cfg(profile: ProfileId, host: &HostId, nic: &str) -> SwitchPortCfg {
    let trunk = |native, allowed: &[u16]| PortMode::Trunk { native, allowed: allowed.iter().copied().collect() };
    match (profile, nic) {
        (ProfileId::MiniPc, _) => SwitchPortCfg { mode: trunk(Some(MGMT_VLAN), &MINIPC_VLANS), ..host_port_cfg() },
        (ProfileId::Ms01, n) if n.starts_with("sfp") => SwitchPortCfg {
            mode: trunk(None, &MS01_BOND_VLANS),
            lacp_group: Some(host.to_string()),
            ..host_port_cfg()
        },
        _ => host_port_cfg(),
    }
}

/// Where [`cable_host`] put a host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cabled {
    /// `rack-0` for MS-01s, else `rack-<n>` for its access switch `access-<n>`.
    pub location: String,
    /// The switch ports its cables landed on.
    pub switch_ports: Vec<(SwitchId, PortName)>,
}

/// Cables `id` the way a technician would: `eth0` into the next free access
/// host port and, for an MS-01, `sfp0`/`sfp1` into the lowest port index free
/// on both cores; then configures those ports ([`technician_port_cfg`]).
/// Leaves the world untouched and fails if there is no room.
pub fn cable_host(w: &mut World, id: &HostId, profile: ProfileId) -> Result<Cabled, String> {
    let (access, port) = free_access_ports(w).into_iter().next().ok_or("no free access switch port")?;
    let mut cables = vec![("eth0", access_id(access), port)];
    if profile == ProfileId::Ms01 {
        let core_port = free_core_ports(w).into_iter().next().ok_or("no free core switch port pair")?;
        cables.push(("sfp0", SwitchId::from(CORE_1), core_port.clone()));
        cables.push(("sfp1", SwitchId::from(CORE_2), core_port));
    }
    for (nic, switch, port) in &cables {
        w.topology.connect(Node::HostPort(id.clone(), PortName::from(*nic)), Node::SwitchPort(switch.clone(), port.clone()));
        w.topology.set_port(switch, port, technician_port_cfg(profile, id, nic));
    }
    Ok(Cabled {
        location: if profile == ProfileId::Ms01 { CORE_RACK.into() } else { format!("rack-{access}") },
        switch_ports: cables.into_iter().map(|(_, s, p)| (s, p)).collect(),
    })
}

/// How many more hosts of `profile` [`cable_host`] can place.
pub fn capacity(w: &World, profile: ProfileId) -> usize {
    let access = free_access_ports(w).len();
    if profile == ProfileId::Ms01 { access.min(free_core_ports(w).len()) } else { access }
}

/// Free host ports on access switches, in cabling order: `access-1 p1`, `p2`, ...
fn free_access_ports(w: &World) -> Vec<(u32, PortName)> {
    let mut switches: Vec<u32> =
        w.topology.switches.keys().filter_map(|s| s.as_str().strip_prefix("access-")?.parse().ok()).collect();
    switches.sort_unstable();
    switches
        .into_iter()
        .flat_map(|n| (1..=ACCESS_HOST_PORTS).map(move |i| (n, port(i))))
        .filter(|(n, p)| w.topology.peer(&Node::SwitchPort(access_id(*n), p.clone())).is_none())
        .collect()
}

/// Core host ports free on both cores, lowest first.
fn free_core_ports(w: &World) -> Vec<PortName> {
    let free = |core: &str, p: &PortName| w.topology.peer(&Node::SwitchPort(SwitchId::from(core), p.clone())).is_none();
    (1..=CORE_HOST_PORTS).map(port).filter(|p| free(CORE_1, p) && free(CORE_2, p)).collect()
}

fn port(i: u32) -> PortName {
    PortName::new(format!("p{i}"))
}

fn access_id(n: u32) -> SwitchId {
    SwitchId::new(format!("access-{n}"))
}

/// Switch-to-switch trunk: every network's VLAN tagged, mgmt native.
fn trunk_cfg(w: &World) -> SwitchPortCfg {
    let allowed = w.networks.values().map(|n| n.vlan).collect();
    SwitchPortCfg { mode: PortMode::Trunk { native: Some(MGMT_VLAN), allowed }, mtu: SWITCH_MTU, lacp_group: None, admin_up: true }
}

fn add_cores(w: &mut World) {
    let trunk = trunk_cfg(w);
    for (id, peer) in [(CORE_1, CORE_2), (CORE_2, CORE_1)] {
        let mut ports: std::collections::BTreeMap<PortName, (u32, SwitchPortCfg)> =
            (1..=CORE_PORTS).map(|i| (port(i), (CORE_PORT_MBPS, host_port_cfg()))).collect();
        ports.insert(PortName::from(INTER_CORE_PORT), (INTER_CORE_MBPS, trunk.clone()));
        w.topology.add_switch(Switch {
            id: SwitchId::from(id),
            model: "core-64x10g-sfp+".into(),
            ports,
            mlag_peer: Some(SwitchId::from(peer)),
            up: true,
        });
    }
    let core1 = |p: &str| Node::SwitchPort(SwitchId::from(CORE_1), PortName::from(p));
    let core2 = Node::SwitchPort(SwitchId::from(CORE_2), PortName::from(INTER_CORE_PORT));
    w.topology.connect(core1(INTER_CORE_PORT), core2);
    let router = SwitchPortCfg {
        mode: PortMode::Trunk { native: None, allowed: ROUTER_VLANS.into() },
        ..host_port_cfg()
    };
    w.topology.set_port(&SwitchId::from(CORE_1), &PortName::from(ROUTER_PORT), router);
    w.topology.connect(Node::Router, core1(ROUTER_PORT));
    w.topology.connect(Node::Client, core1(CLIENT_PORT));
}

/// Adds `access-<n>` with `sfp1` cabled to core-1 and `sfp2` to core-2, both
/// on core port `p(63 - n)`.
fn add_access_switch(w: &mut World, n: u32) {
    let core_port = CORE_HOST_PORTS + 1 - n;
    assert!(core_port >= 1, "the cores have no port left for access-{n}");
    let trunk = trunk_cfg(w);
    let mut ports: std::collections::BTreeMap<PortName, (u32, SwitchPortCfg)> =
        (1..=ACCESS_PORTS).map(|i| (port(i), (ACCESS_PORT_MBPS, host_port_cfg()))).collect();
    for u in 1..=ACCESS_UPLINKS {
        ports.insert(PortName::new(format!("sfp{u}")), (UPLINK_MBPS, trunk.clone()));
    }
    let id = access_id(n);
    w.topology.add_switch(Switch { id: id.clone(), model: "access-48x2.5g-4x10g".into(), ports, mlag_peer: None, up: true });
    for (uplink, core) in [("sfp1", CORE_1), ("sfp2", CORE_2)] {
        let core = SwitchId::from(core);
        w.topology.set_port(&core, &port(core_port), trunk.clone());
        w.topology.connect(Node::SwitchPort(id.clone(), PortName::from(uplink)), Node::SwitchPort(core, port(core_port)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use control::state::{ClusterState, NetState};
    use model::NetworkId;

    fn sp(s: &str, p: &str) -> Node {
        Node::SwitchPort(SwitchId::from(s), PortName::from(p))
    }
    fn hp(h: &str, p: &str) -> Node {
        Node::HostPort(HostId::from(h), PortName::from(p))
    }

    /// The seeded log replayed, as the first leader's no-op will commit it.
    fn replay(d: &Durable<LogCmd>) -> ClusterState {
        let mut s = ClusterState::new();
        for e in &d.log {
            let EntryPayload::Normal(LogCmd { at, cmd }) = &e.payload else { panic!("seed holds commands only") };
            s.apply(*at, cmd.clone()).unwrap_or_else(|err| panic!("entry {} failed: {err}", e.index));
        }
        s
    }

    fn durable_of(actors: &InitialActors, at: &Endpoint) -> Durable<LogCmd> {
        let (_, a) = actors.iter().find(|(e, _)| e == at).expect("actor spawned");
        let any: &dyn std::any::Any = a.as_ref();
        any.downcast_ref::<CpReplica>().expect("a replica").durable().clone()
    }

    #[test]
    fn preset_specs() {
        assert_eq!(Preset::Default.spec(), FleetSpec { ms01: 12, minipc: 24, pi5: 12, rk3588: 12 });
        assert_eq!(Preset::Default.spec().total(), 60);
        assert_eq!(Preset::Scale500.spec(), FleetSpec { ms01: 40, minipc: 200, pi5: 120, rk3588: 140 });
        assert_eq!(Preset::Scale500.spec().total(), 500);
        assert_eq!((Preset::Default.cp_voters(), Preset::Scale500.cp_voters()), (3, 5));
    }

    #[test]
    fn default_fleet_topology() {
        let (w, _) = build_fleet(&Preset::Default.spec(), 3);
        let t = &w.topology;
        let ids: Vec<_> = t.switches.keys().map(|s| s.as_str()).collect();
        assert_eq!(ids, ["access-1", "access-2", "core-1", "core-2"]);
        let core1 = &t.switches[&SwitchId::from(CORE_1)];
        assert_eq!(core1.ports.len(), 65);
        assert_eq!(core1.ports[&PortName::from("p1")].0, 10_000);
        assert_eq!(core1.ports[&PortName::from(INTER_CORE_PORT)].0, 40_000);
        assert_eq!(core1.mlag_peer, Some(SwitchId::from(CORE_2)));
        assert_eq!(t.peer(&sp(CORE_1, INTER_CORE_PORT)), Some(&sp(CORE_2, INTER_CORE_PORT)));
        let access = &t.switches[&SwitchId::from("access-1")];
        assert_eq!(access.ports.values().filter(|(mbps, _)| *mbps == 2_500).count(), 48);
        assert_eq!(access.ports.values().filter(|(mbps, _)| *mbps == 10_000).count(), 4);
        assert_eq!(t.peer(&sp("access-1", "sfp1")), Some(&sp(CORE_1, "p62")));
        assert_eq!(t.peer(&sp("access-1", "sfp2")), Some(&sp(CORE_2, "p62")));
        assert_eq!(t.peer(&sp("access-2", "sfp1")), Some(&sp(CORE_1, "p61")));

        assert_eq!(t.peer(&Node::Router), Some(&sp(CORE_1, ROUTER_PORT)));
        let router = core1.port(&PortName::from(ROUTER_PORT)).unwrap();
        assert_eq!(router.mode, PortMode::Trunk { native: None, allowed: [1, 100, 110].into() });
        assert_eq!(t.peer(&Node::Client), Some(&sp(CORE_1, CLIENT_PORT)));
        assert_eq!(core1.port(&PortName::from(CLIENT_PORT)).unwrap().mode, PortMode::Access(1));

        // MS-01 i: SFPs on core port i, eth0 on the next free access port, eth1 uncabled.
        assert_eq!(t.peer(&hp("ms01-01", "sfp0")), Some(&sp(CORE_1, "p1")));
        assert_eq!(t.peer(&hp("ms01-12", "sfp1")), Some(&sp(CORE_2, "p12")));
        assert_eq!(t.peer(&hp("ms01-01", "eth0")), Some(&sp("access-1", "p1")));
        assert_eq!(t.peer(&hp("ms01-01", "eth1")), None);
        // Then mini PCs, Pis and RK3588s fill access-1 p13..p46 and spill onto access-2.
        assert_eq!(t.peer(&hp("minipc-01", "eth0")), Some(&sp("access-1", "p13")));
        assert_eq!(t.peer(&hp("pi5-10", "eth0")), Some(&sp("access-1", "p46")));
        assert_eq!(t.peer(&hp("pi5-11", "eth0")), Some(&sp("access-2", "p1")));
        assert_eq!(t.peer(&hp("rk3588-12", "eth0")), Some(&sp("access-2", "p14")));
        assert_eq!(t.peer(&sp("access-1", "p47")), None, "p47 and up take no hosts");

        // Host ports are configured for the hosts' templates; host hardware alternates variants.
        let port = |s: &str, p: &str| t.switches[&SwitchId::from(s)].port(&PortName::from(p)).unwrap().clone();
        let trunk = |native, allowed: &[u16], lacp: Option<&str>| SwitchPortCfg {
            mode: PortMode::Trunk { native, allowed: allowed.iter().copied().collect() },
            mtu: 9216,
            lacp_group: lacp.map(String::from),
            admin_up: true,
        };
        assert_eq!(port("access-2", "p14"), host_port_cfg(), "an SBC: access on mgmt");
        assert_eq!(port("access-1", "p1"), host_port_cfg(), "an MS-01's eth0");
        assert_eq!(port("access-1", "p13"), trunk(Some(1), &[1, 20], None), "a mini PC");
        assert_eq!(port(CORE_1, "p1"), trunk(None, &[20, 30, 100, 110], Some("ms01-01")), "an MS-01's bond member");
        assert_eq!(port(CORE_2, "p12"), trunk(None, &[20, 30, 100, 110], Some("ms01-12")));
        assert_eq!(port("access-1", "p47"), host_port_cfg(), "spare ports keep the default");
        // Switch-to-switch links carry every network.
        let uplink = trunk(Some(1), &[1, 20, 30, 100, 110], None);
        assert_eq!((port("access-1", "sfp1"), port(CORE_1, "p62"), port(CORE_1, INTER_CORE_PORT)), (uplink.clone(), uplink.clone(), uplink));
        assert_eq!(w.hosts[&HostId::from("ms01-01")].profile.ram_gb, 96);
        assert_eq!(w.hosts[&HostId::from("ms01-02")].profile.ram_gb, 64);
        assert!(w.hosts.values().all(|h| h.powered && h.join_token.is_none()));
    }

    #[test]
    fn every_host_reaches_the_client_and_router_on_mgmt() {
        let (w, _) = build_fleet(&Preset::Default.spec(), 3);
        let f = Fabric::new(w, 1);
        let mgmt = f.world().networks[&NetworkId::from("mgmt")].clone();
        for id in f.world().hosts.keys() {
            assert!(f.reachable(&hp(id.as_str(), "eth0"), &Node::Client, mgmt.vlan, mgmt.mtu), "{id} -> client");
            assert!(f.reachable(&hp(id.as_str(), "eth0"), &Node::Router, mgmt.vlan, mgmt.mtu), "{id} -> router");
        }
    }

    #[test]
    fn seeded_state_has_every_host_ready_with_rule_roles() {
        let spec = Preset::Default.spec();
        let (w, actors) = build_fleet(&spec, 3);
        let voters: BTreeSet<HostId> = ["ms01-01", "ms01-02", "ms01-03"].map(HostId::from).into();
        let replicas: Vec<_> = actors.iter().filter(|(e, _)| e.svc == Svc::Cp).map(|(e, _)| e.addr.clone()).collect();
        assert_eq!(replicas, voters.iter().map(|v| Addr::Host(v.clone())).collect::<Vec<_>>());
        assert_eq!(actors.iter().filter(|(e, _)| e.svc == Svc::Agent).count(), 60);
        assert_eq!(actors.last().unwrap().0, CLIENT);

        let d = durable_of(&actors, &Endpoint::new(Addr::Host(HostId::from("ms01-01")), Svc::Cp));
        for v in &voters {
            assert_eq!(durable_of(&actors, &Endpoint::new(Addr::Host(v.clone()), Svc::Cp)), d, "same seed everywhere");
        }
        assert_eq!((d.term, &d.voted_for, &d.voters), (1, &None, &voters));
        assert!(d.log.iter().all(|e| e.term == 1));

        let s = replay(&d);
        assert_eq!(s.cp.voters, voters);
        assert_eq!(s.rules.len(), 4);
        assert_eq!(s.hosts.len(), 60);
        assert!(s.hosts.values().all(|h| h.lifecycle == Lifecycle::Ready));
        let host = |id: &str| &s.hosts[&HostId::from(id)];
        assert_eq!(host("ms01-01").roles, [Role::ControlPlane, Role::Storage, Role::Vm, Role::Compute].into());
        assert_eq!(host("ms01-04").roles, seed_rule_roles(ProfileId::Ms01));
        assert_eq!(host("minipc-01").roles, seed_rule_roles(ProfileId::MiniPc));
        assert_eq!(host("pi5-01").roles, [Role::Transcode, Role::Compute].into());
        assert_eq!((host("ms01-07").location.as_str(), host("minipc-01").location.as_str()), ("rack-0", "rack-1"));
        assert_eq!(host("rk3588-01").location, "rack-2");
        let eth1 = host("ms01-01").inventory.nics.iter().find(|n| n.name == "eth1").unwrap();
        assert!(!eth1.link_up, "the inventory is what the host sees: eth1 is uncabled");
        let rule_template = |id: &str| s.rules[id].template.as_deref();
        assert_eq!(
            ["ms01", "minipc", "pi5", "rk3588"].map(rule_template),
            [Some("ms01-standard"), Some("minipc-standard"), Some("sbc-transcode"), Some("sbc-transcode")]
        );
        assert!(s.rules.values().all(|r| s.templates.contains_key(r.template.as_deref().unwrap())));
        assert_eq!(s.networks.keys().map(|n| n.as_str()).collect::<Vec<_>>(), ["mgmt", "migration", "storage", "vm-iot", "vm-lan"]);
        // Every host runs its rule's template, recorded as applied generation 1, addressed uniquely.
        let mut addresses = BTreeSet::new();
        for h in s.hosts.values() {
            assert_eq!(h.template.as_deref(), Some(seed_rule_template(h.profile.id)), "{}", h.id);
            assert_eq!((h.net.desired_generation, h.net.applied_generation, h.net.state), (1, 1, NetState::Applied), "{}", h.id);
            assert_eq!(h.net.desired.as_ref(), Some(&w.hosts[&h.id].applied_net), "{} runs what the log says", h.id);
            assert!(h.ips.values().all(|ip| addresses.insert(*ip)), "{} shares an address", h.id);
        }
        let ip = |h: &str, n: &str| host(h).ips.get(&NetworkId::from(n)).map(|ip| ip.to_string());
        assert_eq!([ip("ms01-01", "mgmt"), ip("ms01-01", "storage"), ip("ms01-01", "migration"), ip("ms01-01", "vm-lan")], [
            Some("10.0.0.2".into()),
            Some("10.20.0.2".into()),
            Some("10.30.0.2".into()),
            None
        ]);
        assert_eq!((ip("minipc-01", "mgmt"), ip("minipc-01", "storage")), (Some("10.0.0.14".into()), Some("10.20.0.14".into())));
        assert_eq!((ip("rk3588-12", "mgmt"), ip("rk3588-12", "storage")), (Some("10.0.0.61".into()), None));
        assert_eq!(s.templates.keys().map(String::as_str).collect::<Vec<_>>(), ["minipc-standard", "ms01-standard", "sbc-transcode"]);
    }

    #[test]
    fn scale_fleet_fits_the_cores() {
        let (w, actors) = build_fleet(&Preset::Scale500.spec(), 5);
        assert_eq!(w.hosts.len(), 500);
        let access = w.topology.switches.keys().filter(|s| s.as_str().starts_with("access-")).count();
        assert_eq!(access, 11, "500 hosts / 46 ports");
        assert_eq!(w.topology.peer(&sp("access-11", "sfp1")), Some(&sp(CORE_1, "p52")));
        assert_eq!(w.topology.peer(&hp("ms01-40", "sfp0")), Some(&sp(CORE_1, "p40")));
        assert_eq!(w.topology.peer(&hp("rk3588-140", "eth0")), Some(&sp("access-11", "p40")));
        assert_eq!(actors.iter().filter(|(e, _)| e.svc == Svc::Cp).count(), 5);
    }

    #[test]
    fn loading_a_fleet_replaces_the_previous_one() {
        let mut e = Engine::new(1);
        e.load_fleet(&FleetSpec { ms01: 3, minipc: 2, pi5: 0, rk3588: 0 }, 3);
        e.run_for(5000);
        e.load_fleet(&FleetSpec { ms01: 1, minipc: 0, pi5: 0, rk3588: 0 }, 1);
        assert_eq!(e.fabric().world().hosts.len(), 1);
        let gone = |h: &str, svc| e.actor::<HostAgent>(&Endpoint::new(Addr::Host(HostId::from(h)), svc)).is_none();
        assert!(gone("minipc-01", Svc::Agent) && gone("ms01-02", Svc::Agent));
        assert!(e.actor::<CpReplica>(&Endpoint::new(Addr::Host(HostId::from("ms01-02")), Svc::Cp)).is_none());
        let seeded = e.fabric().world().hosts[&HostId::from("ms01-01")].cp_durable.clone().unwrap();
        assert_eq!(seeded.voters, [HostId::from("ms01-01")].into(), "the new fleet starts from its own seed");
        assert!(e.run_until(e.now() + 5000, |e| e.true_leader().is_some()));
    }

    #[test]
    fn cable_host_fails_cleanly_when_full() {
        let (mut w, _) = build_fleet(&FleetSpec { ms01: 0, minipc: 46, pi5: 0, rk3588: 0 }, 3);
        assert_eq!(capacity(&w, ProfileId::Pi5), 0);
        let before = w.topology.clone();
        assert!(cable_host(&mut w, &HostId::from("x"), ProfileId::Pi5).is_err());
        assert_eq!(w.topology, before);
    }
}
