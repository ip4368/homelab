//! Control-plane logic and the runtime interface it is written against.
//! This crate must never depend on `sim`: control logic only sees the world
//! through [`runtime::Ctx`] and never reads ground truth.

pub mod agent;
pub mod client;
pub mod controllers;
pub mod proto;
pub mod raft;
pub mod replica;
pub mod runtime;
pub mod state;
