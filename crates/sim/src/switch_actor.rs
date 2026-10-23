//! The management interface of a switch: answers the leader's polls with the
//! switch's own configuration. It sits at `Addr::Switch(id)` / `Svc::Switch`,
//! reachable over the fabric on the mgmt VLAN only while the switch is up.

use control::proto::{mgmt, Msg};
use control::runtime::{Actor, Ctx, Endpoint};

pub struct SwitchActor;

impl Actor for SwitchActor {
    fn on_start(&mut self, _: &mut dyn Ctx) {}

    fn on_message(&mut self, ctx: &mut dyn Ctx, from: Endpoint, msg: Msg) {
        if !matches!(msg, Msg::SwitchPoll) {
            return;
        }
        let Some(os) = ctx.switch() else { return };
        let (switch, config) = (os.switch_id(), os.config());
        let report = Msg::SwitchReport { switch, model: config.model, ports: config.ports, mlag_peer: config.mlag_peer };
        ctx.send(from, mgmt(), report);
    }

    fn on_timer(&mut self, _: &mut dyn Ctx, _: u64) {}
}
