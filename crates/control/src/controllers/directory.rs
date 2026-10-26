//! The address directory: every member host's static addresses, by network,
//! sent to every member agent when it changes and every
//! [`DIRECTORY_EVERY_MS`] regardless. The control logic never reads it; it is
//! for a real host's runtime, which must turn `Addr::Host(id)` into an IP
//! (to reach a CP peer, a replica's source, a flow's target) even before
//! it hears from the leader again after a reboot.

use super::{Action, Volatile};
use crate::proto::Msg;
use crate::runtime::{Addr, Endpoint, SimTime, Svc};
use crate::state::{ClusterState, Lifecycle};
use model::{HostId, NetworkId};
use std::collections::BTreeMap;
use std::net::Ipv4Addr;

/// Resent this often (design §8.3), for a host that missed a change.
pub const DIRECTORY_EVERY_MS: u64 = 30_000;

pub type Directory = BTreeMap<HostId, BTreeMap<NetworkId, Ipv4Addr>>;

/// The addresses of every host that has any and is not removed.
pub fn directory(state: &ClusterState) -> Directory {
    state
        .hosts
        .values()
        .filter(|h| h.lifecycle != Lifecycle::Removed && !h.ips.is_empty())
        .map(|h| (h.id.clone(), h.ips.clone()))
        .collect()
}

pub fn run(state: &ClusterState, v: &Volatile, now: SimTime) -> Vec<Action> {
    let dir = directory(state);
    let due = match &v.directory_sent {
        Some((sent, at)) => *sent != dir || now >= at + DIRECTORY_EVERY_MS,
        None => true,
    };
    if !due || dir.is_empty() {
        return vec![];
    }
    dir.keys()
        .map(|h| {
            let to = Endpoint::new(Addr::Host(h.clone()), Svc::Agent);
            Action::Send(to, crate::proto::mgmt(), Msg::Directory { hosts: dir.clone() })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Command;
    use model::{profile, Inventory, ProfileId, Role};

    fn state_with(hosts: &[(&str, &str)]) -> ClusterState {
        let mut s = ClusterState::new();
        for (id, ip) in hosts {
            let id = HostId::from(*id);
            let cmds = [
                Command::AddPendingHost {
                    id: id.clone(),
                    profile: profile(ProfileId::MiniPc, 0),
                    inventory: Inventory::from_profile(ProfileId::MiniPc, 0),
                    location: "lab".into(),
                },
                Command::ApproveHost { id: id.clone(), roles: [Role::Compute].into() },
                Command::ProvisionHost {
                    id: id.clone(),
                    template: None,
                    ips: [(NetworkId::from("mgmt"), ip.parse().unwrap())].into(),
                },
            ];
            for c in cmds {
                s.apply(0, c).unwrap();
            }
        }
        s
    }

    fn sends(actions: &[Action]) -> Vec<String> {
        actions
            .iter()
            .map(|a| match a {
                Action::Send(Endpoint { addr: Addr::Host(h), svc: Svc::Agent }, _, Msg::Directory { hosts }) => {
                    format!("{h}:{}", hosts.len())
                }
                other => panic!("unexpected {other:?}"),
            })
            .collect()
    }

    #[test]
    fn the_directory_goes_to_every_member_on_change_and_periodically() {
        let s = state_with(&[("a", "10.0.0.2"), ("b", "10.0.0.3")]);
        let mut v = Volatile::new(0, 1);
        let out = run(&s, &v, 1000);
        assert_eq!(sends(&out), ["a:2", "b:2"]);
        for a in &out {
            let Action::Send(to, _, msg) = a else { unreachable!() };
            v.note_sent(to, msg, 1000);
        }
        assert!(run(&s, &v, 2000).is_empty(), "nothing changed");
        assert_eq!(sends(&run(&s, &v, 1000 + DIRECTORY_EVERY_MS)), ["a:2", "b:2"], "resent periodically");
        let grown = state_with(&[("a", "10.0.0.2"), ("b", "10.0.0.3"), ("c", "10.0.0.4")]);
        assert_eq!(sends(&run(&grown, &v, 3000)), ["a:3", "b:3", "c:3"], "resent on change");
    }
}
