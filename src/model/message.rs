use std::net::SocketAddr;

use bytes::Bytes;
use crdts::Dot;
use tap::Pipe;
use uuid7::{uuid7, Uuid};

use crate::{
    model::{Id, LogList, LogOp},
    Actor,
};

/// TODO: MessageSet
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MessageSet {}

/// Message with topic and target information that's ready to be sent
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Envelope {
    pub(crate) addr: SocketAddr,
    pub(crate) topic: String,
    pub(crate) body: Message,
    pub(crate) id: Uuid,
}

/// Direct message
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Message {
    /// Message sent by swim
    Swim(Bytes),
    /// Cluster join request, with own information
    RequestSnapshot,
    /// Snapshot of the current state of the topic
    Snapshot { snapshot: LogList },
}

/// Messages being passed around internally, not produced internally. External
/// messages are also included
pub enum InternalMessage {
    /// Timer event of foca
    Timer(foca::Timer<Id>),
    /// Broadcast various messages
    Broadcast(Vec<u8>),
    // Join()
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Broadcast {
    pub id: Uuid,
    pub data: BroadcastType,
}

impl Broadcast {
    pub fn new_crdt(op: LogOp) -> Self {
        Self {
            id: uuid7(),
            data: BroadcastType::CrdtOp(op),
        }
    }

    pub fn pack(&self) -> Result<BroadcastPacked, bincode::Error> {
        BroadcastPacked::try_from(self)
    }

    pub fn into_packed(self) -> Result<BroadcastPacked, bincode::Error> {
        BroadcastPacked::try_from(self)
    }
}

/// Broadcast types
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[allow(clippy::upper_case_acronyms)]
pub enum BroadcastType {
    CrdtOp(LogOp),
}

impl Broadcast {
    /// Try to get the dot of the message for invalidation
    pub fn dot(&self) -> Option<Dot<Actor>> {
        match &self.data {
            BroadcastType::CrdtOp(op) => op.dot().pipe(Some),
        }
    }
}

impl From<LogOp> for BroadcastType {
    fn from(op: LogOp) -> Self {
        BroadcastType::CrdtOp(op)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BroadcastPacked {
    pub dot: Option<Dot<Actor>>,
    pub data: Vec<u8>,
}

impl BroadcastPacked {
    pub fn new(dot: Option<Dot<Actor>>, data: Vec<u8>) -> Self {
        Self { dot, data }
    }

    pub fn serialize(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(&self)
    }

    pub fn deserialize(&self) -> Result<Broadcast, bincode::Error> {
        bincode::deserialize(&self.data)
    }
}

impl TryFrom<Broadcast> for BroadcastPacked {
    type Error = bincode::Error;

    fn try_from(b: Broadcast) -> Result<Self, Self::Error> {
        BroadcastPacked {
            dot: b.dot(),
            data: bincode::serialize(&b)?,
        }
        .pipe(Ok)
    }
}

impl TryFrom<&Broadcast> for BroadcastPacked {
    type Error = bincode::Error;

    fn try_from(b: &Broadcast) -> Result<Self, Self::Error> {
        BroadcastPacked {
            dot: b.dot(),
            data: bincode::serialize(&b)?,
        }
        .pipe(Ok)
    }
}

impl TryFrom<BroadcastPacked> for BroadcastType {
    type Error = bincode::Error;

    fn try_from(b: BroadcastPacked) -> Result<Self, Self::Error> {
        bincode::deserialize(&b.data)
    }
}

impl AsRef<[u8]> for BroadcastPacked {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

#[test]
fn test_serialize() {
    use std::{net::SocketAddr, str::FromStr};

    use crdts::{Dot, Identifier};
    use uuid7::uuid7;

    use crate::{model::LogOp, Actor, Log};

    let m = Envelope {
        addr: SocketAddr::from_str("127.0.0.1:8000").unwrap(),
        topic: "test".to_owned(),
        body: Message::RequestSnapshot,
        id: uuid7(),
    };
    let b = Broadcast::new_crdt(LogOp::Insert {
        id: Identifier::between(None, None, Dot::new(Actor::random(), 1).into()),
        val: Log::new("oops"),
    })
    .pack()
    .unwrap();

    let mb = bincode::serialize(&m).unwrap();
    let m2 = bincode::deserialize(&mb).unwrap();
    assert_eq!(m, m2);

    let bb = bincode::serialize(&b).unwrap();
    let b2 = bincode::deserialize(&bb).unwrap();
    assert_eq!(b, b2);
}
