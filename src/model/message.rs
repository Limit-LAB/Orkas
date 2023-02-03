use std::net::SocketAddr;

use uuid7::Uuid;

use crate::model::{Id, LogList, LogOp};

/// Message with recipient that's ready to be sent
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SendTo {
    pub msg: MessageSet,
    pub addr: SocketAddr,
}

/// MessageSet
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MessageSet {
    pub message: Vec<Message>,
}

/// A single message
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]

pub struct Message {
    pub sender: Id,
    pub id: Uuid,
    pub topic: String,
    #[serde(flatten)]
    pub body: MessageType,
}

/// Message types
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[allow(clippy::upper_case_acronyms)]
#[serde(tag = "_", rename_all = "snake_case")]
pub enum MessageType {
    /// SWIM message
    SWIM(SWIMMessage),
    /// CRDT message
    CRDT(CRDTMessage),
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
    // Timer(foca::Timer<Id>),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SWIMMessage {
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "op", content = "body", rename_all = "snake_case")]
pub enum CRDTMessage {
    /// Snapshot of the current state of the topic
    Snapshot { snapshot: LogList },
    /// Single Operation
    Op(LogOp),
}

#[cfg(test)]
mod test {
    // use std::time::Duration;

    // use crdts::{num::bigint::ToBigInt, CmRDT};
    // use itertools::Itertools;
    // use tokio::{select, sync::broadcast};

    // use crate::model::{Log, Timestamp, Topic};

    // #[tokio::test]
    // async fn test_1() {
    //     let (op_tx, mut op_rx) = broadcast::channel(1000);

    //     for actor in 0..200u8 {
    //         let op_tx = op_tx.clone();
    //         tokio::spawn(async move {
    //             let mut topic = Topic::default();
    //             let mut iter = 50;
    //             let mut rx = op_tx.subscribe();
    //             loop {
    //                 select! {
    //                     _ = tokio::time::sleep(Duration::from_millis(5)) => {
    //                         iter -= 1;

    //                         let ts = Timestamp::now();
    //                         let new = Log {
    //                             message: format!("{actor}:{iter}"),
    //                             ts,
    //                         };
    //                         let op =
    // topic.insert_id(ts.into_inner().to_bigint().unwrap(), new, actor.into());

    //                         topic.apply(op.clone());
    //                         op_tx.send(op).unwrap();
    //                         // eprint!(".");
    //                         if iter == 0 {
    //                             break;
    //                         }
    //                     },
    //                     op = async { rx.recv().await.unwrap() } => {
    //                         topic.apply(op);
    //                     }
    //                 }
    //             }
    //         });
    //     }

    //     drop(op_tx);

    //     let mut topic = Topic::default();

    //     loop {
    //         let Ok(op) = op_rx.recv().await else { break };
    //         topic.apply(op.clone());
    //     }
    //     eprintln!(".");

    //     topic.iter().for_each(|x| println!("{x:?}"));
    //     topic
    //         .iter_entries()
    //         .enumerate()
    //         .tuple_windows()
    //         .filter(|(a, b)| a.1.1.ts > b.1.1.ts)
    //         .for_each(|((ai, (aid, a)), (bi, (bid, b)))| {
    //             eprintln!("Not sorted at: [{ai}] {a:?} {aid} > [{bi}] {b:?}
    // {bid}",);         })
    // }
}
