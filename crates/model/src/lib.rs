//! Shared domain types for the cluster manager. These are the wire format;
//! ts-rs exports them to `ui/src/gen/`.

pub mod hardware;
pub mod ids;
pub mod inventory;
pub mod ipam;
pub mod lifecycle;
pub mod network;
pub mod roles;
pub mod schedule;
pub mod template;
pub mod validate;
pub mod workload;

pub use hardware::*;
pub use ids::*;
pub use inventory::*;
pub use ipam::*;
pub use lifecycle::*;
pub use network::*;
pub use roles::*;
pub use schedule::*;
pub use template::*;
pub use validate::*;
pub use workload::*;
