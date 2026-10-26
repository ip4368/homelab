//! The UI's client on the lab host: a [`ClientActor`] on a node runtime that
//! reaches the control plane through the VIP, across the lab network.

use crate::topology::VIP;
use control::client::ClientActor;
use control::runtime::{Addr, Svc};
use node::runtime::{NodeConfig, NodeHandle};
use node::transport::AddressBook;
use node::wire::PORT;
use std::net::Ipv4Addr;

/// Starts the client, dialling the VIP at `vip` on `port`.
pub fn spawn_at(vip: Ipv4Addr, port: u16) -> std::io::Result<NodeHandle> {
    let mut book = AddressBook::default();
    book.fixed.insert(Addr::Vip, vip);
    let cfg = NodeConfig { me: Addr::Client, listen: None, port, book };
    NodeHandle::spawn(cfg, |n| n.start(Svc::Client, Box::new(ClientActor::new())))
}

/// Starts the client against the lab's VIP.
pub fn spawn() -> std::io::Result<NodeHandle> {
    spawn_at(VIP, PORT)
}
