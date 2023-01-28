use std::collections::HashSet;

use crdts::ctx::{AddCtx, ReadCtx, RmCtx};

use crate::{Id, Log};

/// Messages
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
    /// Initial request to join a topic cluster
    Join {
        topic: String,
        swim: foca::Message<Id>,
    },
    /// Response to a join request
    JoinResponse {
        // A snapshot of the current state of the topic cluster to quickly populate the new node
        snapshot: ReadCtx<HashSet<String>, u64>,
        // Responding message from foca
        swim: foca::Message<Id>,
    },
    /// Swim messages
    Swim(foca::Message<Id>),
    /// Crdt add ctx
    CrdtAdd { ctx: AddCtx<u64> },
    /// Crdt remove ctx
    CrdtRm { ctx: RmCtx<u64> },
    /// Crdt read ctx
    CrdtRead { ctx: ReadCtx<Log, u64> },
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use crdts::CmRDT;
    use rand::Rng;
    use tap::Pipe;
    use tokio::{select, sync::broadcast};

    use crate::{Log, Topic};

    #[tokio::test]
    async fn test_1() {
        let mut map = HashMap::<String, Topic>::default();

        // let mut map = Map::<String, Orswot<u8, u8>, u8>::default();
        let (op_tx, mut op_rx) = broadcast::channel(1000);

        for actor in 5..15 {
            let op_tx = op_tx.clone();
            tokio::spawn(async move {
                let mut topic = Topic::default();
                let mut j = actor;
                let mut rx = op_tx.subscribe();
                loop {
                    select! {
                        _ = {
                            let s = rand::thread_rng()
                                .gen_range(25..75)
                                .pipe(Duration::from_millis);
                            tokio::time::sleep(s)
                        } => {
                            j -= 1;
                            let now = SystemTime::now();
                            let new = Log {
                                message: format!("{actor}:{j}"),
                                ts: (now.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64).into(),
                            };
                            let op = match topic.iter().position(|x| x.ts >= new.ts) {
                                Some(idx) => topic.insert_index(idx, new, actor),
                                None => topic.append(new, actor),
                            };

                            topic.apply(op.clone());
                            op_tx.send(op).unwrap();
                            if j == 0 {
                                break;
                            }
                        },
                        op = rx.recv() => {
                            if let Ok(op) = op {
                                topic.apply(op);
                            }
                        }
                    }
                }
            });
        }

        drop(op_tx);

        loop {
            let Ok(op) = op_rx.recv().await else { break };
            let topic = map.entry("test".to_owned()).or_default();
            topic.apply(op.clone());
        }
        let topic = map.entry("test".to_owned()).or_default();
        topic.iter().for_each(|x| println!("{x:?}"));
        println!(
            "{}",
            topic.iter().is_sorted_by(|a, b| a.ts.cmp(&b.ts).pipe(Some))
        );

        // println!(
        //     "[1] {:#?}",
        //     map.values()
        //         .flat_map(|x| x.val.iter())
        //         .map(|x| x.val)
        //         .collect::<Vec<_>>()
        // );
        // spawn(|| {
        //     let mut r = Map::new();
        //     r.apply(op);
        //     println!(
        //         "[2] {:#?}",
        //         r.values()
        //             .flat_map(|x| x.val.iter())
        //             .map(|x| x.val)
        //             .collect::<Vec<_>>()
        //     );
        // })
        // .join()
        // .unwrap();
        // map.values().for_each(|x| {
        //     x.val.read().val.iter().for_each(|x| {
        //         println!("{:?}", x);
        //     })
        // });
    }
}
