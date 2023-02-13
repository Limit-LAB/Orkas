//! Background task of SWIM

use std::{
    borrow::Cow,
    collections::HashSet,
    fmt::Debug,
    net::SocketAddr,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use bytes::{Bytes, BytesMut};
use color_eyre::Result;
use foca::{BincodeCodec, Foca, Invalidates, Notification, Runtime};
use kanal::AsyncSender;
use rand::{rngs::StdRng, thread_rng, SeedableRng};
use tap::Pipe;
use tokio::{select, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info_span, trace};
use uuid7::{uuid7, Uuid};

use crate::{
    codec::{bincode_option, BincodeOptions},
    consts::{DEFAULT_BUFFER_SIZE, DEFAULT_CHANNEL_SIZE},
    model::Id,
    tasks::{event_aggregator, make_event_channel, Context, EventConsumer, EventProducer},
    util::{ok_or_break, ok_or_continue, ok_or_warn},
    Broadcast, BroadcastPack, BroadcastTag, Envelope, Event, InternalMessage, Message,
};

#[allow(clippy::upper_case_acronyms)]
pub type SWIM = Foca<Id, BincodeCodec<BincodeOptions>, StdRng, BroadcastHandler>;

/// A temporary holder of swim. This is then spawned via [`Swim::spawn`] to
/// create a long-living task.
#[derive(Debug)]
pub(crate) struct Swim {
    ctx: Context,
    swim: SWIM,
    topic: String,
    chan: (EventProducer, EventConsumer),
}

impl Swim {
    pub fn new(local_addr: SocketAddr, ctx: Context, topic: String) -> Result<Self> {
        let (event, event_rx) = make_event_channel(DEFAULT_CHANNEL_SIZE);

        let swim = SWIM::with_custom_broadcast(
            Id::from(local_addr),
            ctx.config.swim.clone(),
            StdRng::from_rng(thread_rng())?,
            BincodeCodec(bincode_option()),
            BroadcastHandler::new(),
        );

        Ok(Self {
            ctx,
            swim,
            topic,
            chan: (event, event_rx),
        })
    }

    pub fn spawn(self) -> SwimJobHandle {
        let Swim {
            ctx,
            mut swim,
            topic,
            chan: (event, event_rx),
        } = self;

        let (internal_tx, internal_rx) =
            kanal::bounded_async::<InternalMessage>(DEFAULT_CHANNEL_SIZE);
        let (external_tx, external_rx) = kanal::bounded_async::<Bytes>(DEFAULT_CHANNEL_SIZE);

        let token = ctx.cancel_token.child_token();
        let cancel_token = token.child_token();

        let event_aggregator = tokio::spawn(event_aggregator(
            ctx.config.clone(),
            internal_tx.clone(),
            event_rx,
        ))
        .pipe(Arc::new);

        let handle = tokio::spawn(async move {
            let mut rt = ctx.swim_runtime(&topic);
            let mut buf = BytesMut::with_capacity(DEFAULT_BUFFER_SIZE);

            loop {
                select! {
                    _ = token.cancelled() => {
                        break Ok(());
                    }
                    internal = internal_rx.recv() => {
                        trace!(?internal, "internal");

                        let _s = info_span!("swim.internal", id = ?swim.identity()).entered();

                        let msg = ok_or_break!("internal", internal, topic);
                        match msg {
                            InternalMessage::Timer(event) => {
                                ok_or_warn!("timer", swim.handle_timer(event, &mut rt));
                            }
                            InternalMessage::Broadcast(b) => {
                                let packed = ok_or_continue!("broadcast", BroadcastPack::pack_with(&mut buf, &b));

                                ok_or_warn!("broadcast", swim.add_broadcast(&packed.data))
                            }
                        }
                    }
                    external = external_rx.recv() => {
                        trace!(?external, "external");
                        let _s = info_span!("external", id = ?swim.identity()).entered();

                        let msg = ok_or_break!("swim", external, topic);
                        ok_or_warn!("handle_data", swim.handle_data(msg.deref(), &mut rt))
                    }
                };

                ok_or_break!("flush", rt.flush().await);
            }
        })
        .pipe(Arc::new);

        SwimJobHandle {
            cancel_token,
            internal_tx,
            external_tx,
            event_aggregator,
            handle,
            event,
        }
    }
}

impl Deref for Swim {
    type Target = SWIM;

    fn deref(&self) -> &Self::Target {
        &self.swim
    }
}

impl DerefMut for Swim {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.swim
    }
}

/// Shim runtime that implements [`foca::Runtime`] trait.
#[must_use]
pub(crate) struct OrkasRuntime<'a> {
    topic: Cow<'a, str>,
    ctx: &'a Context,
    msg_buf: Vec<Envelope>,
}

impl OrkasRuntime<'_> {
    /// Flush buffered messages to outbound channel
    #[inline]
    pub(crate) async fn flush(&mut self) -> Result<()> {
        for msg in self.msg_buf.drain(..) {
            self.ctx.msg.send(msg).await?;
        }
        Ok(())
    }
}

impl Drop for OrkasRuntime<'_> {
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

impl<'a> Runtime<Id> for OrkasRuntime<'a> {
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

            ok_or_warn!(
                "submit_after",
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
    event_aggregator: Arc<JoinHandle<Result<()>>>,
    event: EventProducer,
}

/// State of the background SWIM job
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwimState {
    /// All components are running
    Running,

    /// Some components has stopped
    Partial {
        /// Main SWIM loop is running
        main_running: bool,

        /// Event aggregator is running
        event_aggregator_running: bool,
    },

    /// All components has stopped
    Stopped,
}

impl SwimJobHandle {
    /// Send internal messages to SWIM, which may be timer events or new
    /// broadcast
    pub async fn send_internal(&self, msg: impl Into<InternalMessage>) -> Result<()> {
        self.internal_tx.send(msg.into()).await?;
        Ok(())
    }

    /// Send external messages (just bytes as foca will decode it later) to SWIM
    pub async fn send_external(&self, msg: Bytes) -> Result<()> {
        self.external_tx.send(msg).await?;
        Ok(())
    }

    pub fn send_event(&self, event: Event) -> Result<()> {
        self.event.send(event).map_err(Into::into)
    }

    pub async fn broadcast(&self, b: impl Into<Broadcast>) -> Result<()> {
        self.send_internal(InternalMessage::Broadcast(b.into()))
            .await
    }

    pub fn state(&self) -> SwimState {
        let main_running = !self.handle.is_finished();
        let event_aggregator_running = !self.event_aggregator.is_finished();

        if main_running && event_aggregator_running {
            SwimState::Running
        } else if main_running || event_aggregator_running {
            SwimState::Partial {
                main_running,
                event_aggregator_running,
            }
        } else {
            SwimState::Stopped
        }
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
    pub(crate) fn swim_runtime<'a>(&'a self, topic: impl Into<Cow<'a, str>>) -> OrkasRuntime<'a> {
        OrkasRuntime {
            topic: topic.into(),
            ctx: self,
            msg_buf: Vec::new(),
        }
    }
}

impl Invalidates for BroadcastTag {
    fn invalidates(&self, other: &Self) -> bool {
        // Two same broadcasts always invalidate each other (Or is it?)
        self.id == other.id
    }
}

impl Invalidates for BroadcastPack {
    fn invalidates(&self, other: &Self) -> bool {
        self.tag.invalidates(&other.tag)
    }
}

#[derive(Debug, Default)]
pub struct BroadcastHandler {
    seen: HashSet<Uuid>,
    buf: BytesMut,
}

impl BroadcastHandler {
    pub fn new() -> Self {
        Self {
            seen: HashSet::with_capacity(DEFAULT_CHANNEL_SIZE),
            buf: BytesMut::with_capacity(DEFAULT_BUFFER_SIZE),
        }
    }

    /// Test if a broadcast is seen.
    pub fn seen(&self, id: &Uuid) -> bool {
        self.seen.contains(id)
    }

    /// Mark broadcast as seen.
    pub fn saw(&mut self, id: Uuid) {
        self.seen.insert(id);
    }

    /// Read a broadcast from the buffer, unpack it and test if it has been seen
    /// before. If not, copy the buffer so that it can be emitted again.
    fn handle_broadcast(
        &mut self,
        data: &mut impl bytes::Buf,
    ) -> Result<Option<(Bytes, BroadcastTag, Broadcast)>, bincode::Error> {
        let chunk = data.chunk();
        self.buf.clear();

        if data.remaining() < 8 {
            return Ok(None);
        }

        let Some((len, tag, broadcast)) = BroadcastPack::unpack(&data)? else {
            return Ok(None);
        };

        if self.seen(&tag.id) {
            return Ok(None);
        }

        // Include 8 bytes of `len` itself
        let whole = len as usize + 8;

        self.buf.extend_from_slice(&chunk[..whole]);
        let bytes = self.buf.split().freeze();

        data.advance(whole);

        Ok(Some((bytes, tag, broadcast)))
    }
}

impl foca::BroadcastHandler<Id> for BroadcastHandler {
    type Broadcast = BroadcastPack;
    type Error = bincode::Error;

    fn receive_item(
        &mut self,
        mut data: impl bytes::Buf,
    ) -> std::result::Result<Option<Self::Broadcast>, Self::Error> {
        let _s = tracing::info_span!("broadcast_handler").entered();

        let Some((bytes, tag, broadcast)) = self.handle_broadcast(&mut data)? else { return Ok(None) };

        debug!(id = %tag.id, "Fresh broadcast");
        trace!(?broadcast);

        self.saw(tag.id);

        match broadcast {
            Broadcast::CrdtOp(_) => todo!(),
            Broadcast::Events(e) => {
                for event in e.into_inner() {
                    tracing::debug!(event = ?event, "Event received");
                    // TODO: Handle event
                }
            }
        }

        trace!(broadcast = ?data.chunk(), len = data.chunk().len(), "Broadcast received");

        tag.pack_with(bytes).map(Some)
    }
}

#[test]
fn test_handle_broadcast() {
    use crate::model::*;

    let broadcast = Broadcast::Events(vec![Event::new(Log::random())].into());
    let pack = BroadcastPack::pack(&broadcast).unwrap();

    let mut handler = BroadcastHandler::new();
    let (bytes, tag, broadcast2) = handler
        .handle_broadcast(&mut pack.as_ref())
        .unwrap()
        .unwrap();

    assert_eq!(pack.as_ref(), &bytes[..]);
    assert_eq!(pack.tag, tag);
    assert_eq!(broadcast, broadcast2);
}
