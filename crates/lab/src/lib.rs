//! The real VM lab: switches, VMs, the services a homelab network has, the
//! faults a person could inflict on hardware, and the ground truth seen from
//! outside the machines. It never touches cluster state.

pub mod switch;
pub mod client;
pub mod cloudinit;
pub mod qemu;
pub mod services;
pub mod topology;
pub mod manager;
