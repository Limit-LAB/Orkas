//! Background task of SWIM

use std::{borrow::Cow, fmt::Debug, ops::Deref, pin::pin, sync::Arc};

use bytes::Bytes;
use color_eyre::Result;
use crdts::SyncedCmRDT;
use foca::{BincodeCodec, BroadcastHandler, Foca, Invalidates, Notification, Runtime};
use futures::future::{select, Either};
use kanal::AsyncSender;
use rand::rngs::StdRng;
use tap::{Pipe, Tap, TapOptional};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, info_span, trace};
use uuid7::uuid7;

use crate::{
    codec::{bincode_option, try_decode, BincodeOptions},
    model::Id,
    tasks::{Context, DEFAULT_CHANNEL_SIZE},
    util::{ok_or_break, ok_or_warn},
    Broadcast, BroadcastPacked, BroadcastType, Envelope, InternalMessage, Message,
};

#[allow(clippy::upper_case_acronyms)]
pub type SWIM = Foca<Id, BincodeCodec<BincodeOptions>, StdRng, OrkasBroadcastHandler>;

/// Shim runtime that implements [`foca::Runtime`] trait.
#[must_use]
pub(crate) struct SwimRuntime<'a> {
    topic: Cow<'a, str>, // Workaround for initialize with borrowed str in `src/lib.rs`
    ctx: &'a Context,
    msg_buf: Vec<Envelope>,
}

impl SwimRuntime<'_> {
    /// Flush buffered messages to outbound channel
    pub(crate) async fn flush(&mut self) -> Result<()> {
        for msg in std::mem::take(&mut self.msg_buf) {
            self.ctx.msg.send(msg).await?;
        }
        Ok(())
    }
}

impl Drop for SwimRuntime<'_> {
    fn drop(&mut self) {
        if self.msg_buf.is_empty() {
            return;
        }
        let send = self.ctx.msg.clone();
        let msgs = std::mem::take(&mut self.msg_buf);

        tokio::spawn(async move {
            for msg in msgs {
                send.send(msg).await.unwrap();
            }
        });
    }
}

impl<'a> Runtime<Id> for SwimRuntime<'a> {
    // TODO: handle notification
    fn notify(&mut self, _notification: foca::Notification<Id>) {
        match _notification {
            Notification::MemberUp(id) | Notification::Rejoin(id) => {
                if let Some(waiter) = self.ctx.waiters.remove(&id.addr()) {
                    waiter.value().notify()
                }
            }
            Notification::MemberDown(_) => {}
            Notification::Active => {}
            Notification::Idle => {}
            Notification::Defunct => {}
        }
    }

    fn send_to(&mut self, to: Id, data: &[u8]) {
        self.msg_buf.push(Envelope {
            addr: to.addr(),
            topic: self.topic.as_ref().to_owned(),
            body: Message::Swim(data.to_vec().into()),
            id: uuid7(),
        })
    }

    fn submit_after(&mut self, event: foca::Timer<Id>, after: std::time::Duration) {
        let Some(topic) = self.ctx.topics.get(self.topic.as_ref()) else { return };
        let sender = topic.value().swim.internal_tx.clone();

        tokio::spawn(async move {
            tokio::time::sleep(after).await;
            // Ignore if current target no longer running after sleep
            ok_or_warn!(
                "swim.submit_after",
                sender.send(InternalMessage::Timer(event)).await
            )
        });
    }
}

#[derive(Debug, Clone)]
pub struct SwimJobHandle {
    cancel_token: CancellationToken,
    internal_tx: AsyncSender<InternalMessage>,
    external_tx: AsyncSender<Bytes>,
    handle: Arc<JoinHandle<Result<()>>>,
}

impl SwimJobHandle {
    /// Send internal messages to SWIM, which may be timer events or new
    /// broadcast
    pub async fn send_internal(&self, msg: InternalMessage) -> Result<()> {
        self.internal_tx.send(msg).await?;
        Ok(())
    }

    /// Send external messages (just bytes as foca will decode it later) to SWIM
    pub async fn send_external(&self, msg: Bytes) -> Result<()> {
        self.external_tx.send(msg).await?;
        Ok(())
    }

    pub async fn broadcast(&self, msg: Broadcast) -> Result<()> {
        // self.send_internal(InternalMessage::Broadcast(msg)).await
        Ok(())
    }

    pub fn running(&self) -> bool {
        !self.handle.is_finished()
    }

    /// Stop the task. Note that this will not stop the task immediately.
    /// Instead, a cancel request is issued and the task will be stopped when it
    /// is safe to do so, i.e., when all pending tasks are finished.
    pub fn stop(&self) {
        // self.external_tx.close();
        // self.internal_tx.close();
        self.cancel_token.cancel();
    }
}

impl Context {
    pub(crate) fn swim_runtime<'a>(&'a self, topic: impl Into<Cow<'a, str>>) -> SwimRuntime<'a> {
        SwimRuntime {
            topic: topic.into(),
            ctx: self,
            msg_buf: Vec::new(),
        }
    }
}

impl Invalidates for BroadcastPacked {
    fn invalidates(&self, other: &Self) -> bool {
        // Two same broadcasts always invalidate each other (Or is it?)
        if self.id == other.id {
            return true;
        }
        let b: Option<bool> = try { self.dot? >= other.dot? };
        b.unwrap_or(false)
    }
}

pub struct OrkasBroadcastHandler {
    ctx: Context,
    topic: String,
}

impl OrkasBroadcastHandler {
    pub fn new(topic: impl Into<String>, ctx: Context) -> Self {
        Self {
            topic: topic.into(),
            ctx,
        }
    }
}

impl BroadcastHandler<Id> for OrkasBroadcastHandler {
    type Broadcast = BroadcastPacked;
    type Error = bincode::Error;

    fn receive_item(
        &mut self,
        mut data: impl bytes::Buf,
    ) -> std::result::Result<Option<Self::Broadcast>, Self::Error> {
        let _s = tracing::info_span!("swim.broadcast_handler").entered();

        trace!(target: "swim.broadcast_handler", "{:?}", data.chunk());

        let topic = &self.topic;

        if let Ok(Some(broadcast)) = try_decode::<Broadcast>(&mut data, bincode_option()) {
            debug!(broadcast = ?broadcast, "Broadcast received");
            match broadcast.data {
                BroadcastType::CrdtOp(op) => {
                    if let Some(topic) = self.ctx.topics.get(topic) {
                        info!(?op, "Applying crdt op");
                        topic.value().logs.synced_apply(op);
                        info!(list_len = ?topic.value().logs.len());
                    } else {
                        info!(topic, "Non-exist topic, ignore");
                    }
                }
            }
            None
        } else {
            try_decode::<BroadcastPacked>(&mut data, bincode_option())?
                .tap(|_| debug!(target: "swim.broadcast_handler", "Broadcast pack received"))
                .filter(|pack| !self.ctx.seen(&pack.id))
                .tap_some(|pack| {
                    debug!(target: "swim.broadcast_handler", ?pack, "Fresh broadcast pack");
                    self.ctx.saw(pack.id)
                })
        }
        .pipe(Ok)
    }
}

pub(crate) fn spawn_swim(topic: String, mut swim: SWIM, ctx: Context) -> SwimJobHandle {
    let (internal_tx, internal_rx) = kanal::bounded_async::<InternalMessage>(DEFAULT_CHANNEL_SIZE);
    let (external_tx, external_rx) = kanal::bounded_async::<Bytes>(DEFAULT_CHANNEL_SIZE);
    let cancel_token = ctx.cancel_token.child_token();
    let token = cancel_token.clone();

    let handle = tokio::spawn(async move {
        let mut rt = ctx.swim_runtime(&topic);

        loop {
            debug!(id = ?swim.identity());

            if token.is_cancelled() {
                // TODO: graceful shutdown by reading all remaining messages and close the
                // channel.
                break;
            }

            let (l, r) = (pin!(internal_rx.recv()), pin!(external_rx.recv()));

            match select(l, r).await {
                Either::Left((internal, _)) => {
                    trace!(?internal, "swim.internal");

                    let _s = info_span!("swim.internal", id = ?swim.identity()).entered();

                    let msg = ok_or_break!("swi.internal", internal, topic);
                    match msg {
                        InternalMessage::Timer(event) => {
                            ok_or_warn!("swim.timer", swim.handle_timer(event, &mut rt));
                        }
                        InternalMessage::Broadcast(b) => {
                            ok_or_warn!("swim.add_broadcast", swim.add_broadcast(&b))
                        }
                    }
                }
                Either::Right((external, _)) => {
                    trace!(?external, "swim.external");
                    let _s = info_span!("swim.external", id = ?swim.identity()).entered();

                    let msg = ok_or_break!("swim", external, topic);
                    ok_or_warn!("swim.handle_data", swim.handle_data(msg.deref(), &mut rt))
                }
            };

            ok_or_break!("swim.flush", rt.flush().await);
        }

        Ok(())
    })
    .pipe(Arc::new);

    SwimJobHandle {
        cancel_token,
        internal_tx,
        external_tx,
        handle,
    }
}
