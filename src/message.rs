use std::collections::HashSet;

use crdts::ctx::{AddCtx, ReadCtx, RmCtx};

use crate::{Id, Log};

/// MessageSet
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct MessageSet {
    pub message: Vec<MessageType>,
}

/// A single message
#[derive(Debug, serde::Serialize, serde::Deserialize)]

pub struct Message {
    pub sender: Id,
    #[serde(flatten)]
    pub message: MessageType,
}

/// Message types
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "_", content = "body")]
pub enum MessageType {
    SWIM(SWIMMessage),
    CRDT(CRDTMessage),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum SWIMMessage {
    /// Initial request to join a topic cluster
    Join {
        topic: String,
        swim: foca::Message<Id>,
    },
    /// Response to a join request
    JoinResponse {
        // A snapshot of the current state of the topic cluster to quickly populate the new node
        // TODO: Use state-based updating instead of manually snapshotting
        snapshot: ReadCtx<HashSet<String>, u64>,
        // Responding message from foca
        swim: foca::Message<Id>,
    },
    /// Cluster membership (SWIM) messages
    Cluster {
        topic: String,
        msg: foca::Message<Id>,
    },
}
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum CRDTMessage {
    /// Crdt add ctx
    CrdtAdd { ctx: AddCtx<u64> },
    /// Crdt remove ctx
    CrdtRm { ctx: RmCtx<u64> },
    /// Crdt read ctx
    CrdtRead { ctx: ReadCtx<Log, u64> },
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use crdts::{num::bigint::ToBigInt, CmRDT};
    use itertools::Itertools;
    use tokio::{select, sync::broadcast};

    use crate::{Log, Timestamp, Topic};

    #[tokio::test]
    async fn test_1() {
        let (op_tx, mut op_rx) = broadcast::channel(1000);

        for actor in 0..200 {
            let op_tx = op_tx.clone();
            tokio::spawn(async move {
                let mut topic = Topic::default();
                let mut iter = 50;
                let mut rx = op_tx.subscribe();
                loop {
                    select! {
                        _ = tokio::time::sleep(Duration::from_millis(5)) => {
                            iter -= 1;

                            let ts = Timestamp::now();
                            let new = Log {
                                message: format!("{actor}:{iter}"),
                                ts,
                            };
                            let op = topic.insert_id(ts.value().to_bigint().unwrap(), new, actor);


                            topic.apply(op.clone());
                            op_tx.send(op).unwrap();
                            // eprint!(".");
                            if iter == 0 {
                                break;
                            }
                        },
                        op = async { rx.recv().await.unwrap() } => {
                            topic.apply(op);
                        }
                    }
                }
            });
        }

        drop(op_tx);

        let mut topic = Topic::default();

        loop {
            let Ok(op) = op_rx.recv().await else { break };
            topic.apply(op.clone());
        }
        eprintln!(".");

        topic.iter().for_each(|x| println!("{x:?}"));
        topic
            .iter_entries()
            .enumerate()
            .tuple_windows()
            .filter(|(a, b)| a.1.1.ts > b.1.1.ts)
            .for_each(|((ai, (aid, a)), (bi, (bid, b)))| {
                eprintln!("Not sorted at: [{ai}] {a:?} {aid} > [{bi}] {b:?} {bid}",);
            })
    }
}
