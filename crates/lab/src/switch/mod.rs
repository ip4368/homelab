//! `labswitch`: the lab's VLAN switches, in userspace (the lab host's kernel
//! cannot do VLAN-filtering bridges). [`core`] is the forwarding logic;
//! [`io`] puts it on the wire: VM NICs over unix datagram sockets (QEMU's
//! `dgram` netdev) and the lab host itself over a tap.

pub mod core;
pub mod frame;
pub mod io;
