mod_use::mod_use![conn, swim, event];

use std::{io, net::SocketAddr, ops::Deref, sync::Arc, time::Duration};

use color_eyre::{eyre::bail, Result};
use crdts::SyncedCmRDT;
use crossbeam_skiplist::{map::Entry, SkipMap};
use tap::{Pipe, Tap, TapOptional};
use tokio::{
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener,
    },
    sync::Notify,
    task::JoinSet,
    time::timeout,
};
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};
use tracing::debug;

use crate::{
    codec::{EnvelopeSink, EnvelopeStream},
    consts::DEFAULT_CHANNEL_SIZE,
    model::{Actor, Envelope, Event, State, Topic},
    util::{CRDTReader, CRDTUpdater},
    Log, LogList, Orkas, OrkasConfig,
};

type Inbound = EnvelopeStream<OwnedReadHalf>;
type Outbound = EnvelopeSink<OwnedWriteHalf>;

/// Context of Orkas.
///
/// Contains all the channel sender. This type is relatively cheap to clone and
/// all fields are behind reference count thus all instances targets to the same
/// objects.
#[derive(Clone, Debug)]
pub struct ContextRef {
    inner: Arc<Context>,
}

impl Deref for ContextRef {
    type Target = Context;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug)]
pub struct Context {
    pub msg: kanal::AsyncSender<Envelope>,
    pub conn_inbound: kanal::AsyncSender<Inbound>,
    pub conn_outbound: kanal::AsyncSender<(SocketAddr, Outbound)>,
    pub waiters: SkipMap<SocketAddr, Notify>,
    pub config: Arc<OrkasConfig>,
    pub cancel_token: CancellationToken,
    topics: SkipMap<String, Topic>,
    actor: Actor,
}

impl Context {
    pub fn wrap(self) -> ContextRef {
        ContextRef {
            inner: Arc::new(self),
        }
    }
}

impl ContextRef {
    pub fn cancelled(&self) -> WaitForCancellationFuture<'_> {
        self.cancel_token.cancelled()
    }

    /// Cancel the background tasks.
    pub fn cancel(&self) {
        self.cancel_token.cancel();
        self.topics.iter().for_each(|t| t.value().stop())
    }

    /// Stop the background tasks and channels.
    pub fn stop(&self) {
        self.cancel_token.cancel();
        self.topics.iter().for_each(|t| t.value().swim.stop());
        self.msg.close();
        self.conn_inbound.close();
        self.conn_outbound.close();
    }

    pub fn state(&self) -> State {
        todo!()
    }

    pub fn log(&self, topic: impl AsRef<str>, log: Log) -> Result<()> {
        let topic = topic.as_ref();
        let Some(t) = self.topics.get(topic) else { bail!("Topic does not exist") };

        t.value().swim.send_event(Event::new(log))?;

        Ok(())
    }

    pub async fn update<F>(&self, topic: impl AsRef<str>, updater: F) -> Result<bool>
    where
        F: CRDTUpdater,
        F::Error: Send + Sync + 'static,
    {
        let topic = topic.as_ref();
        let Some(t) = self.topics.get(topic) else { bail!("Topic does not exist") };
        let logs = &t.value().logs;
        let Some(op) = updater.update(logs, self.actor)? else { return Ok(false) };
        logs.synced_apply(op.clone());
        debug!(?op, topic, "update sent");

        t.value().swim.broadcast(op).await?;

        Ok(true)
    }

    pub fn get_topic(&self, topic: impl AsRef<str>) -> Option<TopicEntry<'_>> {
        self.topics
            .get(topic.as_ref())
            .map(TopicEntry::prepare(self))
    }

    pub fn has_topic(&self, topic: impl AsRef<str>) -> bool {
        self.topics.contains_key(topic.as_ref())
    }

    pub(crate) fn insert_topic(&self, name: impl Into<String>, topic: Topic) -> TopicEntry<'_> {
        self.topics
            .insert(name.into(), topic)
            .pipe(TopicEntry::prepare(self))
    }

    /// Remove a topic from the context. This will also terminate the topic's
    /// background tasks include swim and limlog.
    pub(crate) fn remove_topic(&self, topic: impl AsRef<str>) -> Option<TopicEntry<'_>> {
        self.topics
            .remove(topic.as_ref())
            .map(TopicEntry::prepare(self))
            .tap_some(|x| x.entry.value().stop())
    }

    /// Wait for node with corresponding address to join or rejoin.
    pub async fn wait_for(&self, addr: SocketAddr, dur: Duration) -> bool {
        self.waiters
            .get_or_insert_with(addr, Notify::new)
            .value()
            .notified()
            .pipe(|fut| timeout(dur, fut))
            .await
            .is_ok()
    }
}

pub struct TopicEntry<'a> {
    entry: Entry<'a, String, Topic>,
    ctx: &'a ContextRef,
}

impl<'a> TopicEntry<'a> {
    fn prepare(ctx: &'a ContextRef) -> impl FnOnce(Entry<'a, String, Topic>) -> Self {
        move |entry| Self { entry, ctx }
    }

    /// Get the CRDT struct.
    pub(crate) fn crdt(&self) -> &LogList {
        &self.entry.value().logs
    }

    /// Get the swim job handle.
    pub(crate) fn swim(&self) -> &SwimJobHandle {
        &self.entry.value().swim
    }

    /// Get the swim state.
    pub fn swim_state(&self) -> SwimState {
        self.swim().state()
    }

    /// Read from the CRDT.
    pub fn read<R: CRDTReader>(&self, reader: R) -> R::Return {
        reader.read(self.crdt())
    }

    /// Get the topic name.
    pub fn name(&self) -> &str {
        self.entry.key()
    }

    /// Get the context.
    pub fn ctx(&self) -> &ContextRef {
        self.ctx
    }

    /// A stream of all the logs.
    pub fn subscribe(&self) -> limlog::Reader {
        self.entry.value().map.reader()
    }

    /// A sink to write logs to.
    pub fn writer(&self) -> limlog::Writer {
        self.entry.value().map.writer()
    }
}

pub async fn spawn_background(config: Arc<OrkasConfig>) -> io::Result<Orkas> {
    let listener = TcpListener::bind(config.bind).await?;
    let addr = listener.local_addr()?.tap(|address| {
        debug!(?address, "Listening");
    });

    let (conn_inbound, inbound_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);
    let (conn_outbound, outbound_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);
    let (msg, msg_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);

    let cancel_token = CancellationToken::new();
    let topics = SkipMap::new();
    let waiters = SkipMap::new();

    let ctx = Context {
        actor: Actor::random(),
        msg,
        conn_inbound,
        conn_outbound,
        topics,
        waiters,
        config,
        cancel_token,
    }
    .wrap();

    let mut join_set = JoinSet::new();

    join_set.spawn(listener_task(listener, ctx.clone()));
    join_set.spawn(inbound_task(inbound_rx, ctx.clone()));
    join_set.spawn(outbound_task(msg_rx, outbound_rx, ctx.clone()));

    Orkas {
        ctx,
        addr,
        join_set,
    }
    .pipe(Ok)
}
