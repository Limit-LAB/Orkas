use std::{
    future::ready,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use color_eyre::Result;
use futures::StreamExt;
use kanal::{AsyncReceiver, AsyncSender, Sender};
use tracing::instrument;

use crate::{
    model::{Broadcast, Event, InternalMessage, OrkasConfig},
    util::ok_or_break,
};

pub type EventProducer = Sender<Event>;

/// A wrapper of ringbuf consumer that provides helpers
#[derive(Debug)]
pub struct EventConsumer(pub(crate) AsyncReceiver<Event>);

// pub type ByteProducer = HeapProducer<u8>;
// pub type ByteConsumer = HeapConsumer<u8>;

pub fn make_event_channel(size: usize) -> (EventProducer, EventConsumer) {
    // AsyncHeapRb::new(size).split()
    let (tx, rx) = kanal::bounded_async(size);
    (tx.to_sync(), EventConsumer(rx))
}

impl EventConsumer {
    pub async fn drain(&mut self) -> Vec<Event> {
        let mut vec = Vec::with_capacity(self.len());

        self.stream()
            .take(self.len())
            .for_each(|x| {
                vec.push(x);
                ready(())
            })
            .await;
        vec
    }
}

impl Deref for EventConsumer {
    type Target = AsyncReceiver<Event>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for EventConsumer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Aggregate outgoing events and pack them
#[instrument(skip(rx))]
pub(crate) async fn event_aggregator(
    cfg: Arc<OrkasConfig>,
    internal_tx: AsyncSender<InternalMessage>,
    mut rx: EventConsumer,
) -> Result<()> {
    // Check and emit every timewindow
    let mut interval = tokio::time::interval(cfg.event_withhold);

    loop {
        interval.tick().await;

        if rx.is_empty() {
            continue;
        }
        let events = rx.drain().await;
        let res = internal_tx
            .send(InternalMessage::Broadcast(Broadcast::new_events(
                events.into(),
            )))
            .await;
        ok_or_break!("event_aggregator", res);
    }
}
