//! Data wrapper and type aliases used in the project.

use crdts::{List, MVReg, Map};
use serde::{Deserialize, Serialize};

use crate::Log;

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
