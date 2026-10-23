//! Shared domain types for the cluster manager. These are the wire format;
//! ts-rs exports them to `ui/src/gen/`.

pub mod hardware;
pub mod ids;
pub mod inventory;
pub mod network;
pub mod roles;

pub use hardware::*;
pub use ids::*;
pub use inventory::*;
pub use network::*;
pub use roles::*;
