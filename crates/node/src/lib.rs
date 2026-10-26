//! The real runtime for the control plane: `homelabd` on every host, and the
//! UI client in the server. Implements [`control::runtime::Ctx`] over real
//! time and TCP ([`runtime`], [`transport`]) and [`control::runtime::HostOs`]
//! over a Linux machine ([`linux`]).

pub mod linux;
pub mod runtime;
pub mod testing;
pub mod transport;
pub mod wire;
