use std::net::SocketAddr;

use bytes::Bytes;
use uuid7::Uuid;

use crate::model::{Id, LogList, LogOp};

/// TODO: MessageSet
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MessageSet {}

/// Message with topic and target information that's ready to be sent
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Envelope {
    pub(crate) addr: SocketAddr,
    pub(crate) topic: String,
    pub(crate) body: Bytes,
}

/// Messages being passed around internally, not produced internally. External
/// messages are also included
pub enum InternalMessage {
    /// Timer event of foca
    Timer(foca::Timer<Id>),
    /// Broadcast various messages
    Broadcast(Broadcast),
}

/// Message types
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[allow(clippy::upper_case_acronyms)]
#[serde(tag = "_", rename_all = "snake_case")]
pub enum Broadcast {
    /// Cluster join request, with own information
    Join {
        /// ID of joiner
        id: Id,
        /// [`Message::Announce`] sent by swim
        ///
        /// [`Message::Announce`]: foca::Message::Announce
        swim_data: Vec<u8>,
    },
    JoinResponse {
        /// Message id to be responded to
        respond_to: Uuid,
        /// Response by swim
        swim_data: Vec<u8>,
        /// Snapshot of the current state of the topic
        snapshot: LogList,
    },
    /// Snapshot of the current state of the topic
    CrdtSnapshot { snapshot: LogList },
    /// Single CRDT Operation
    CrdtOp(LogOp),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "op", content = "body", rename_all = "snake_case")]
pub enum CRDTMessage {
    /// Snapshot of the current state of the topic
    Snapshot { snapshot: LogList },
    /// Single Operation
    Op(LogOp),
}
