mod_use::mod_use![conn, swim, event];

use std::{net::SocketAddr, sync::Arc, time::Duration};

use color_eyre::{eyre::bail, Result};
use crdts::SyncedCmRDT;
use crossbeam_skiplist::{map::Entry, SkipMap};
use futures::future::join_all;
use tap::{Pipe, Tap};
use tokio::{
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener,
    },
    task::JoinHandle,
};
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};
use tracing::{debug, info, warn};

use crate::{
    codec::{EnvelopeSink, EnvelopeStream},
    consts::DEFAULT_CHANNEL_SIZE,
    model::{Actor, Envelope, State, Topic},
    util::{CRDTReader, CRDTUpdater, Flag},
    Event, Log, LogList, OrkasConfig,
};

type Inbound = EnvelopeStream<OwnedReadHalf>;
type Outbound = EnvelopeSink<OwnedWriteHalf>;

/// Context of Orkas.
///
/// Contains all the channel sender. This type is relatively cheap to clone and
/// all fields are behind reference count thus all instances targets to the same
/// objects.
#[derive(Clone, Debug)]
pub struct Context {
    pub msg: kanal::AsyncSender<Envelope>,
    pub conn_inbound: kanal::AsyncSender<Inbound>,
    pub conn_outbound: kanal::AsyncSender<(SocketAddr, Outbound)>,
    pub waiters: Arc<SkipMap<SocketAddr, Flag>>,
    pub config: Arc<OrkasConfig>,
    topics: Arc<SkipMap<String, Topic>>,
    actor: Actor,
    cancel_token: CancellationToken,
}

impl Context {
    pub fn cancelled(&self) -> WaitForCancellationFuture<'_> {
        self.cancel_token.cancelled()
    }

    pub fn close_all(&self) {
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
        self.topics.get(topic.as_ref()).map(TopicEntry::new)
    }

    pub fn insert_topic(&self, name: impl Into<String>, topic: Topic) -> TopicEntry<'_> {
        self.topics.insert(name.into(), topic).pipe(TopicEntry::new)
    }

    pub fn remove_topic(&self, topic: impl AsRef<str>) -> Option<TopicEntry<'_>> {
        self.topics.remove(topic.as_ref()).map(TopicEntry::new)
    }

    /// Wait for node with corresponding address to join or rejoin.
    pub async fn wait_for(&self, addr: SocketAddr, dur: Duration) -> bool {
        let f = self
            .waiters
            .get_or_insert_with(addr, Flag::new)
            .value()
            .clone();

        f.timeout(dur).await.is_ok()
    }
}

pub struct TopicEntry<'a> {
    entry: Entry<'a, String, Topic>,
}

impl<'a> TopicEntry<'a> {
    fn new(entry: Entry<'a, String, Topic>) -> Self {
        Self { entry }
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
}

pub async fn spawn_background(config: Arc<OrkasConfig>) -> Result<Background> {
    let (conn_inbound, inbound_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);
    let (conn_outbound, outbound_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);
    let (msg, msg_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);

    let cancel_token = CancellationToken::new();
    let topics = SkipMap::new().pipe(Arc::new);
    let waiters = SkipMap::new().pipe(Arc::new);

    let ctx = Context {
        actor: Actor::random(),
        msg,
        conn_inbound,
        conn_outbound,
        topics,
        waiters,
        config,
        cancel_token,
    };

    let listener = TcpListener::bind(ctx.config.bind).await?;
    let addr = listener.local_addr()?.tap(|addr| {
        debug!(?addr, "Listening on");
    });

    let listener = listener
        .pipe(|l| listener_task(l, ctx.clone()))
        .pipe(tokio::spawn);

    let inbound = tokio::spawn(inbound_task(inbound_rx, ctx.clone()));
    let outbound = tokio::spawn(outbound_task(msg_rx, outbound_rx, ctx.clone()));

    Background {
        ctx,
        addr,
        stopped: false,
        handles: vec![inbound, outbound, listener],
    }
    .pipe(Ok)
}

// TODO: enum Status { Starting, Running, Stopped }
pub struct Background {
    ctx: Context,
    addr: SocketAddr,
    handles: Vec<JoinHandle<Result<()>>>,
    stopped: bool,
}

impl Drop for Background {
    fn drop(&mut self) {
        if !self.stopped {
            warn!("Background tasks are not properly stopped. Forcefully stopping.");
            self.force_stop();
        }
    }
}

impl Background {
    /// Check if all background tasks are still running.
    pub fn is_running(&self) -> bool {
        !(self.stopped
            || self.ctx.cancel_token.is_cancelled()
            || self.handles.iter().any(|x| x.is_finished()))
    }

    /// Get the context of the background tasks.
    pub fn ctx(&self) -> &Context {
        &self.ctx
    }

    /// Get the address of the background tasks.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Issue a cancellation request to the background tasks.
    pub fn cancel(&self) {
        self.ctx.close_all();
    }

    /// Get the cancellation token of the background tasks.
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.ctx.cancel_token
    }

    /// Forcefully shutdown the background tasks with [`JoinHandle::abort`].
    ///
    /// [`JoinHandle::abort`]: tokio::task::JoinHandle::abort
    pub fn force_stop(&mut self) {
        if self.stopped {
            return;
        }
        self.stopped = true;
        self.ctx.close_all();
        self.handles.iter().for_each(|x| x.abort());
    }

    /// Wait for the background tasks to finish. Use with
    /// [`BackgroundHandle::cancel`] to gracefully shutdown the background
    /// tasks.
    pub async fn stop(mut self) -> Vec<Result<()>> {
        info!("Stopping background tasks...");
        self.stopped = true;
        self.ctx.close_all();
        join_all(std::mem::take(&mut self.handles))
            .await
            .into_iter()
            .map(|x| match x {
                Ok(Ok(_)) => Ok(()),
                // Normal error
                Ok(Err(e)) => Err(e),
                // Panicked or cancelled
                Err(e) => bail!("Background task quit: {e}"),
            })
            .collect::<Vec<_>>()
    }
}
