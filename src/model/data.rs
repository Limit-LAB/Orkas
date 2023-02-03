//! Data wrapper and type aliases used in the project.

use std::{
    fmt::Display,
    net::SocketAddr,
    time::{SystemTime, UNIX_EPOCH},
};

use crdts::{list::Op, List};
use serde::{Deserialize, Serialize};

use crate::model::Log;

/// Represent operations on a topic.
pub type LogOp = Op<Log, Actor>;

/// Type for CRDT to identify actors and differentiate between them. This is
/// used everytime on updating the logs map.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Actor(u64);
// pub type Topic = Map<Timestamp, MVReg<Log, Actor>, Actor>;

impl Actor {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn into_inner(self) -> u64 {
        self.0
    }

    pub fn random() -> Self {
        Self(rand::random())
    }
}

impl<T: Into<u64>> From<T> for Actor {
    fn from(val: T) -> Self {
        Self(val.into())
    }
}
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

    pub fn into_inner(self) -> u64 {
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
fn test_ts_serde_overhead() {
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

impl Id {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.addr, self.rev)
    }
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
