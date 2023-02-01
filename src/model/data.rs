//! Data wrapper and type aliases used in the project.

use std::{
    net::SocketAddr,
    time::{SystemTime, UNIX_EPOCH},
};

use crdts::List;
use serde::{Deserialize, Serialize};

use crate::model::Log;

/// Type for CRDT to identify actors and differentiate between them. This is
/// used everytime on updating the logs map.
pub type Actor = u64;

/// Represent a single topic which is then being stored in the global topics
/// map, and can be updated separately and synced throughout a single topic
/// cluster.
pub type Topic = List<Log, Actor>;
// pub type Topic = Map<Timestamp, MVReg<Log, Actor>, Actor>;

/// UNIX Timestamp. Uses 64 bit unsigned internally.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Timestamp(u64);

impl Timestamp {
    pub fn now() -> Self {
        Self(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        )
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}

impl From<u64> for Timestamp {
    fn from(val: u64) -> Self {
        Self(val)
    }
}

impl From<Timestamp> for u64 {
    fn from(val: Timestamp) -> Self {
        val.0
    }
}

#[test]
fn test_ts_serde() {
    assert_eq!(
        bincode::serialize(&Timestamp(0)).unwrap(),
        bincode::serialize(&0u64).unwrap()
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Id {
    addr: SocketAddr,
    rev: u64, // TODO: use other id, like uuid
}

impl foca::Identity for Id {
    fn renew(&self) -> Option<Self> {
        let Self { addr, rev } = self;

        Some(Self {
            addr: *addr,
            rev: rev.wrapping_add(1),
        })
    }

    fn has_same_prefix(&self, other: &Self) -> bool {
        self.addr == other.addr
    }
}

impl From<SocketAddr> for Id {
    fn from(addr: SocketAddr) -> Self {
        Self { addr, rev: 0 }
    }
}