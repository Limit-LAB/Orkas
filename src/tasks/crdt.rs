//! Background task of CRDT

use color_eyre::{eyre::Context as EyreCtx, Result};
use crdts::CmRDT;
use dashmap::mapref::entry::Entry;
use kanal::AsyncReceiver;
use tracing::info;

use crate::{model::CRDTMessage, tasks::Context};

pub(super) async fn crdt_task(
    recv: AsyncReceiver<(String, CRDTMessage)>,
    ent: Context,
) -> Result<()> {
    loop {
        let (topic, message) = recv
            .recv()
            .await
            .wrap_err("All senders of CRDT are dropped")?;
        match message {
            CRDTMessage::Snapshot { snapshot } => {
                match ent.topics.entry(topic.clone()) {
                    Entry::Occupied(mut e) => {
                        e.get_mut().logs = snapshot;
                    }
                    Entry::Vacant(_) => {
                        info!(
                            target: "crdt",
                            topic,
                            "Non-exist topic, ignore"
                        );
                    }
                };
            }
            CRDTMessage::Op(op) => {
                if let Some(mut topic) = ent.topics.get_mut(&topic) {
                    topic.logs.apply(op);
                }
            }
        }
    }
}
