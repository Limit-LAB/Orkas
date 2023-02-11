use std::net::SocketAddr;

use bincode::Options;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crdts::Dot;
use tap::{Pipe, TryConv};
use tracing::trace;
use uuid7::{uuid7, Uuid};

use crate::{
    codec::{bincode_option, try_decode},
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
    Broadcast(Bytes),
    // Join()
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Broadcast {
    CrdtOp(LogOp),
}

impl Broadcast {
    pub fn new_crdt(op: LogOp) -> Self {
        Broadcast::CrdtOp(op)
    }

    /// Try to get the dot of the message for invalidation
    pub fn dot(&self) -> Option<Dot<Actor>> {
        match self {
            Broadcast::CrdtOp(op) => op.dot().pipe(Some),
        }
    }

    pub fn tag(&self) -> BroadcastTag {
        BroadcastTag::new(self.dot())
    }

    pub fn pack(&self) -> Result<BroadcastPacked, bincode::Error> {
        let tag = self.tag();

        let opt = bincode_option();
        let len: usize = opt
            .serialized_size(&tag)?
            .try_conv::<usize>()
            .expect("Broadcast size too big")
            + opt
                .serialized_size(&self)?
                .try_conv::<usize>()
                .expect("Broadcast size too big");

        let mut buf = BytesMut::with_capacity(len + 8);
        buf.put_u64(len as _);
        let mut buf = buf.writer();

        opt.serialize_into(&mut buf, &tag)?;
        opt.serialize_into(&mut buf, &self)?;

        let data = buf.into_inner().freeze();
        trace!("Broadcast packed: {:?}", data);

        Ok(BroadcastPacked { tag, data })
    }
}

/// Broadcast types

impl From<LogOp> for Broadcast {
    fn from(op: LogOp) -> Self {
        Broadcast::CrdtOp(op)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BroadcastPacked {
    pub tag: BroadcastTag,
    pub data: Bytes,
}

impl BroadcastPacked {
    pub fn new(dot: Option<Dot<Actor>>, data: impl Into<Bytes>) -> Self {
        Self {
            tag: BroadcastTag::new(dot),
            data: data.into(),
        }
    }

    pub fn serialize(&self) -> Result<InternalMessage, bincode::Error> {
        Ok(InternalMessage::Broadcast(self.data.clone()))
    }

    pub fn deserialize(data: &[u8]) -> Result<Option<(BroadcastTag, Broadcast)>, bincode::Error> {
        let mut buf = data.as_ref();
        let len = buf.get_u64() as usize;

        if buf.remaining() < len {
            return Ok(None);
        }

        let Some(tag) = try_decode::<BroadcastTag>(&mut buf, bincode_option())? else { return Ok(None) };
        let Some(bc) = try_decode::<Broadcast>(&mut buf, bincode_option())? else { return Ok(None) };

        Ok(Some((tag, bc)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BroadcastTag {
    pub dot: Option<Dot<Actor>>,
    pub id: Uuid,
}

impl BroadcastTag {
    pub fn new_dot(dot: Dot<Actor>) -> Self {
        Self {
            dot: Some(dot),
            id: uuid7(),
        }
    }

    pub fn new(dot: Option<Dot<Actor>>) -> Self {
        Self { dot, id: uuid7() }
    }

    pub fn pack(&self, data: Bytes) -> Result<BroadcastPacked, bincode::Error> {
        Ok(BroadcastPacked {
            tag: self.clone(),
            data,
        })
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

#[test]
fn test_pack() {
    use crdts::Identifier;

    use crate::Log;

    let b = Broadcast::new_crdt(LogOp::Insert {
        id: Identifier::between(None, None, Dot::new(Actor::random(), 1).into()),
        val: Log::new("oops"),
    });

    let p = b.pack().unwrap();
    let bytes = p.data.as_ref();
    let (tag, b2) = BroadcastPacked::deserialize(bytes).unwrap().unwrap();

    assert_eq!(tag, p.tag);
    assert_eq!(b, b2);
}
