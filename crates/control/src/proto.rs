//! Messages exchanged between actors. Later tasks add variants.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Msg {
    Ping(u64),
    Pong(u64),
}
