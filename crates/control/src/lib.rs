//! Control-plane logic and the runtime interface it is written against.
//! This crate must never depend on `sim`: control logic only sees the world
//! through [`runtime::Ctx`] and never reads ground truth.

pub mod proto;
pub mod raft;
pub mod runtime;
pub mod state;
