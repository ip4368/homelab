use serde::{Deserialize, Serialize};
use ts_rs::TS;

macro_rules! id_newtype {
    ($($(#[$m:meta])* $name:ident),+ $(,)?) => {$(
        $(#[$m])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, TS)]
        #[ts(export)]
        pub struct $name(pub String);

        impl $name {
            pub fn new(s: impl Into<String>) -> Self { Self(s.into()) }
            pub fn as_str(&self) -> &str { &self.0 }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(&self.0) }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self { Self(s.to_string()) }
        }
    )+};
}

id_newtype!(HostId, SwitchId, PortName, NetworkId);

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn ids_order_lexically_and_serialize_transparently() {
        let set: BTreeSet<HostId> = ["b", "a", "c"].into_iter().map(HostId::from).collect();
        assert_eq!(set.iter().map(|h| h.as_str()).collect::<Vec<_>>(), ["a", "b", "c"]);
        assert_eq!(serde_json::to_string(&SwitchId::new("core-1")).unwrap(), "\"core-1\"");
        let back: PortName = serde_json::from_str("\"sfp0\"").unwrap();
        assert_eq!(back, PortName::from("sfp0"));
        assert_eq!(NetworkId::from("mgmt").to_string(), "mgmt");
    }
}
