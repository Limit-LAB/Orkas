use std::net::SocketAddr;

use bincode::Options;
use bytes::Bytes;
use crdts::Dot;
use tap::{Pipe, Tap};
use tracing::trace;
use uuid7::{uuid7, Uuid};

use crate::{
    codec::bincode_option,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InternalMessage {
    /// Timer event of foca
    Timer(foca::Timer<Id>),
    /// Broadcast various messages
    Broadcast(Vec<u8>),
    // Join()
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Broadcast {
    pub data: BroadcastType,
}

impl Broadcast {
    pub fn new_crdt(op: LogOp) -> Self {
        Self {
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
    pub id: Uuid,
    pub data: Vec<u8>,
}

impl BroadcastPacked {
    pub fn new(dot: Option<Dot<Actor>>, data: Vec<u8>) -> Self {
        Self {
            dot,
            data,
            id: uuid7(),
        }
    }

    pub fn serialize(&self) -> Result<InternalMessage, bincode::Error> {
        bincode_option()
            .serialize(&self)
            .map(InternalMessage::Broadcast)
    }

    pub fn deserialize(&self) -> Result<Broadcast, bincode::Error> {
        self.try_into()
    }
}

impl TryFrom<Broadcast> for BroadcastPacked {
    type Error = bincode::Error;

    fn try_from(b: Broadcast) -> Result<Self, Self::Error> {
        (&b).try_into()
    }
}

impl TryFrom<&Broadcast> for BroadcastPacked {
    type Error = bincode::Error;

    fn try_from(b: &Broadcast) -> Result<Self, Self::Error> {
        BroadcastPacked {
            dot: b.dot(),
            data: bincode_option().serialize(&b)?,
            id: uuid7(),
        }
        .tap(|b| trace!(target: "message", bytes=?b.data, "Broadcast to BroadcastPack"))
        .pipe(Ok)
    }
}

impl TryFrom<BroadcastPacked> for Broadcast {
    type Error = bincode::Error;

    fn try_from(b: BroadcastPacked) -> Result<Self, Self::Error> {
        (&b).try_into()
    }
}

impl TryFrom<&BroadcastPacked> for Broadcast {
    type Error = bincode::Error;

    fn try_from(b: &BroadcastPacked) -> Result<Self, Self::Error> {
        trace!(target: "message", bytes=?b.data, "BroadcastPack to Broadcast");
        bincode_option().deserialize(&b.data)
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

    let b = Broadcast::new_crdt(LogOp::Insert {
        id: Identifier::between(None, None, Dot::new(Actor::random(), 1).into()),
        val: Log::new("oops"),
    })
    .pack()
    .unwrap();

    let bb = bincode_option().serialize(&b).unwrap();
    let b2 = bincode_option().deserialize(&bb).unwrap();
    assert_eq!(b, b2);

    let m = Envelope {
        addr: SocketAddr::from_str("127.0.0.1:8000").unwrap(),
        topic: "test".to_owned(),
        body: Message::Swim(bb.into()),
        id: uuid7(),
    };

    let mb = bincode_option().serialize(&m).unwrap();
    let m2 = bincode_option().deserialize(&mb).unwrap();

    assert_eq!(m, m2);
}
