//! Join: an unjoined agent sends `JoinRequest`s with a token. A known,
//! unexpired token creates a `Pending` host from the reported inventory
//! ([`on_request`], answered by the replica). The periodic [`run`] then
//! approves pending hosts that match an auto-approve rule, moves approved
//! hosts to `Provisioning`, tells their agents the roles, and marks them
//! `Ready` on their first heartbeat after that.

use super::{Action, Volatile};
use crate::proto::{mgmt, Msg};
use crate::runtime::{Addr, Endpoint, SimTime, Svc};
use crate::state::{ClusterState, Command, HostObj, Lifecycle, ObjectRef};
use model::{eligible, profile, HostId, Inventory};
use std::collections::BTreeSet;

/// At most one "join rejected" event per host this often, however often its agent retries.
pub const REJECT_EVENT_EVERY_MS: u64 = 60_000;
/// A joining host's location until something knows better (presets, LLDP).
pub const UNKNOWN_LOCATION: &str = "unknown";
pub const INVALID_TOKEN: &str = "invalid token";

/// What the leader does with a `JoinRequest`.
#[derive(Debug, Clone, PartialEq)]
pub enum JoinOutcome {
    /// The host already exists: answer `Ok` now.
    Accepted,
    /// Propose this command and answer `Ok` once it applies.
    Add(Command),
    /// An `Add` for this host is in flight. Stay silent; the agent retries.
    InProgress,
    /// Answer with `reason`, and propose `event` if there is one.
    Rejected { reason: String, event: Option<Command> },
}

/// Decides a `JoinRequest` from `host`. Records rejections in `v` to rate-limit their events.
pub fn on_request(
    state: &ClusterState,
    v: &mut Volatile,
    now: SimTime,
    host: &HostId,
    token: &str,
    inventory: Inventory,
) -> JoinOutcome {
    if state.hosts.get(host).is_some_and(|h| h.lifecycle != Lifecycle::Removed) {
        return JoinOutcome::Accepted;
    }
    let from = Addr::Host(host.clone());
    if v.pending_joins.values().any(|p| p.from.addr == from) {
        return JoinOutcome::InProgress;
    }
    if !state.tokens.get(token).is_some_and(|t| now < t.expires_at) {
        let quiet = v.join_rejected_at.get(host).is_some_and(|&at| now < at + REJECT_EVENT_EVERY_MS);
        let event = (!quiet).then(|| {
            v.join_rejected_at.insert(host.clone(), now);
            Command::Event { entity: ObjectRef::host(host), message: format!("join rejected for {host}: {INVALID_TOKEN}") }
        });
        return JoinOutcome::Rejected { reason: INVALID_TOKEN.into(), event };
    }
    JoinOutcome::Add(Command::AddPendingHost {
        id: host.clone(),
        profile: profile(inventory.profile, inventory.variant),
        inventory,
        location: UNKNOWN_LOCATION.into(),
    })
}

pub fn run(state: &ClusterState, v: &Volatile) -> Vec<Action> {
    let mut out = vec![];
    for host in state.hosts.values() {
        let id = &host.id;
        match host.lifecycle {
            Lifecycle::Pending => out.extend(auto_approve(state, host)),
            // Task 14 pushes the host's network config here, before provisioning.
            Lifecycle::Approved => {
                out.push(Action::Propose(Command::SetLifecycle { id: id.clone(), lifecycle: Lifecycle::Provisioning }))
            }
            Lifecycle::Provisioning if heartbeat_since(v, id, host.last_transition) => {
                out.push(Action::Propose(Command::SetLifecycle { id: id.clone(), lifecycle: Lifecycle::Ready }))
            }
            Lifecycle::Provisioning => {
                let agent = Endpoint::new(Addr::Host(id.clone()), Svc::Agent);
                out.push(Action::Send(agent, mgmt(), Msg::Approved { roles: host.roles.clone() }));
            }
            _ => {}
        }
    }
    out
}

fn heartbeat_since(v: &Volatile, host: &HostId, since: SimTime) -> bool {
    v.last_heartbeat.get(host).is_some_and(|&at| at > since)
}

/// Approval by the first rule (by id) for the host's profile, keeping only
/// the roles the host is eligible for, plus an event per dropped role. The
/// approval comes first, so once it applies the events are not asked for again.
fn auto_approve(state: &ClusterState, host: &HostObj) -> Vec<Action> {
    let Some(rule) = state.rules.values().find(|r| r.profile == host.profile.id) else { return vec![] };
    let mut roles = BTreeSet::new();
    let mut events = vec![];
    for &role in &rule.roles {
        match eligible(&host.profile, role) {
            Ok(()) => {
                roles.insert(role);
            }
            Err(reason) => events.push(Action::Propose(Command::Event {
                entity: ObjectRef::host(&host.id),
                message: format!("auto-approve rule {}: dropped role {role:?} for {}: {reason}", rule.id, host.id),
            })),
        }
    }
    let mut out = vec![Action::Propose(Command::ApproveHost { id: host.id.clone(), roles })];
    out.extend(events);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controllers::PendingJoin;
    use model::{ProfileId, Role};

    fn hid(s: &str) -> HostId {
        HostId::from(s)
    }

    fn inv(p: ProfileId, variant: u8) -> Inventory {
        Inventory::from_profile(p, variant)
    }

    fn with_token(expires_in: u64) -> ClusterState {
        let mut s = ClusterState::new();
        s.apply(0, Command::CreateJoinToken { token: "tok".into(), ttl_ms: expires_in }).unwrap();
        s
    }

    fn add(s: &mut ClusterState, id: &str, p: ProfileId, variant: u8) {
        let JoinOutcome::Add(cmd) = on_request(s, &mut Volatile::new(0, 1), 0, &hid(id), "tok", inv(p, variant))
        else {
            panic!("expected Add")
        };
        s.apply(0, cmd).unwrap();
    }

    fn proposals(actions: &[Action]) -> Vec<Command> {
        actions.iter().filter_map(|a| if let Action::Propose(c) = a { Some(c.clone()) } else { None }).collect()
    }

    #[test]
    fn valid_token_adds_a_pending_host_from_the_inventory() {
        let s = with_token(1000);
        let out = on_request(&s, &mut Volatile::new(0, 1), 999, &hid("m1"), "tok", inv(ProfileId::Pi5, 1));
        assert_eq!(
            out,
            JoinOutcome::Add(Command::AddPendingHost {
                id: hid("m1"),
                profile: profile(ProfileId::Pi5, 1),
                inventory: inv(ProfileId::Pi5, 1),
                location: UNKNOWN_LOCATION.into(),
            })
        );
    }

    #[test]
    fn known_host_is_accepted_whatever_the_token_and_in_flight_join_waits() {
        let mut s = with_token(1000);
        let mut v = Volatile::new(0, 1);
        v.pending_joins.insert(7, PendingJoin { from: Endpoint::new(Addr::Host(hid("m2")), Svc::Agent), term: 1 });
        assert_eq!(on_request(&s, &mut v, 0, &hid("m2"), "tok", inv(ProfileId::MiniPc, 0)), JoinOutcome::InProgress);
        add(&mut s, "m1", ProfileId::MiniPc, 0);
        assert_eq!(on_request(&s, &mut v, 5000, &hid("m1"), "gone", inv(ProfileId::MiniPc, 0)), JoinOutcome::Accepted);
    }

    #[test]
    fn unknown_or_expired_token_is_rejected_with_a_rate_limited_event() {
        let s = with_token(1000);
        let mut v = Volatile::new(0, 1);
        let ask = |v: &mut Volatile, at, host: &str, token| on_request(&s, v, at, &hid(host), token, inv(ProfileId::MiniPc, 0));
        let event = |host: &str| Command::Event {
            entity: ObjectRef::host(&hid(host)),
            message: format!("join rejected for {host}: invalid token"),
        };
        let rejected = |event| JoinOutcome::Rejected { reason: INVALID_TOKEN.into(), event };

        assert_eq!(ask(&mut v, 1000, "a", "tok"), rejected(Some(event("a"))), "expired at 1000");
        assert_eq!(ask(&mut v, 6000, "a", "nope"), rejected(None), "within a minute: answer only");
        assert_eq!(ask(&mut v, 6000, "b", "nope"), rejected(Some(event("b"))), "limited per host");
        assert_eq!(ask(&mut v, 1000 + REJECT_EVENT_EVERY_MS, "a", "nope"), rejected(Some(event("a"))));
    }

    #[test]
    fn rule_approves_with_eligible_roles_and_reports_dropped_ones() {
        let mut s = with_token(1000);
        let roles = [Role::Storage, Role::Transcode, Role::Vm].into();
        s.apply(0, Command::UpsertRule { id: "sbc".into(), profile: ProfileId::Pi5, roles, template: None }).unwrap();
        add(&mut s, "pi", ProfileId::Pi5, 0); // SD card, 8 GB: no storage, no vm
        add(&mut s, "mini", ProfileId::MiniPc, 0); // no rule: left for a human
        let cmds = proposals(&run(&s, &Volatile::new(0, 1)));
        assert_eq!(cmds.len(), 3, "{cmds:?}");
        assert_eq!(cmds[0], Command::ApproveHost { id: hid("pi"), roles: [Role::Transcode].into() });
        let Command::Event { entity, message } = &cmds[1] else { panic!("{cmds:?}") };
        assert_eq!(*entity, ObjectRef::host(&hid("pi")));
        assert_eq!(message, "auto-approve rule sbc: dropped role Storage for pi: storage needs a non-SD, non-eMMC disk");
        assert!(matches!(&cmds[2], Command::Event { message, .. } if message.contains("role Vm")), "{cmds:?}");
        for c in cmds {
            s.apply(0, c).unwrap();
        }
        assert_eq!(s.hosts[&hid("pi")].lifecycle, Lifecycle::Approved);
    }

    #[test]
    fn approved_host_provisions_then_is_ready_on_its_next_heartbeat() {
        let mut s = with_token(1000);
        add(&mut s, "m1", ProfileId::MiniPc, 0);
        let roles: BTreeSet<Role> = [Role::Compute].into();
        s.apply(1, Command::ApproveHost { id: hid("m1"), roles: roles.clone() }).unwrap();
        let mut v = Volatile::new(0, 1);
        v.last_heartbeat.insert(hid("m1"), 1500); // heard from while still approved
        let provision = Command::SetLifecycle { id: hid("m1"), lifecycle: Lifecycle::Provisioning };
        assert_eq!(proposals(&run(&s, &v)), std::slice::from_ref(&provision));
        s.apply(2000, provision).unwrap();

        let agent = Endpoint::new(Addr::Host(hid("m1")), Svc::Agent);
        assert_eq!(run(&s, &v), [Action::Send(agent, mgmt(), Msg::Approved { roles })], "tell the agent, wait");
        v.last_heartbeat.insert(hid("m1"), 2500);
        let ready = Command::SetLifecycle { id: hid("m1"), lifecycle: Lifecycle::Ready };
        assert_eq!(run(&s, &v), [Action::Propose(ready.clone())]);
        s.apply(2500, ready).unwrap();
        assert!(run(&s, &v).is_empty());
    }
}
