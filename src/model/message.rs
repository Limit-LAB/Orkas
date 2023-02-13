use std::net::SocketAddr;

use bytes::Bytes;
use crdts::Dot;
use uuid7::Uuid;

use crate::{
    model::{Id, LogList, LogOp},
    Actor, Events,
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
    Broadcast(Broadcast),
}

/// Body of broadcast message
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Broadcast {
    CrdtOp(LogOp),
    Events(Events),
}

/// The broadcast type being used internally for foca to invalidate and
/// distribute. Only the `data` part will be sent so it's crucial to put
/// everything needed into buffer. The structure is:
///
/// ```text
/// +---------+-------------------------+------------------+
/// |   Len   | BroadcastTag (Metadata) | Broadcast (Body) |
/// +---------+-------------------------+------------------+
/// |  8 (BE) | <----------------- Len ------------------> |
/// +---------+--------------------------------------------+
/// ```
///
/// After deserialize from broadcast bytes received, a copy of tag is put inside
/// the `BroadcastPack` struct, so that the tag can be used to invalidate the
/// previous message.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BroadcastPack {
    pub tag: BroadcastTag,
    pub data: Bytes,
}

/// Metadata of broadcast message
#[derive(Debug, Copy, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BroadcastTag {
    pub dot: Option<Dot<Actor>>,
    pub id: Uuid,
}
