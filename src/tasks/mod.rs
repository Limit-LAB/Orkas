mod_use::mod_use![conn, swim];

use std::{net::SocketAddr, sync::Arc, time::Duration};

use color_eyre::{eyre::bail, Result};
use crdts::SyncedCmRDT;
use crossbeam_skiplist::{SkipMap, SkipSet};
use futures::future::join_all;
use tap::{Pipe, Tap};
use tokio::{
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener,
    },
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid7::Uuid;

use crate::{
    codec::{MessageSink, MessageStream},
    model::{Actor, Envelope, State, Topic},
    util::{CRDTUpdater, Flag},
    Broadcast, OrkasConfig,
};

type Inbound = MessageStream<OwnedReadHalf>;
type Outbound = MessageSink<OwnedWriteHalf>;

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
    pub topics: Arc<SkipMap<String, Topic>>,
    pub waiters: Arc<SkipMap<SocketAddr, Flag>>,
    pub config: Arc<OrkasConfig>,
    seen: Arc<SkipSet<Uuid>>,
    actor: Actor,

    cancel_token: CancellationToken,
}

impl Context {
    pub fn close_all(&self) {
        self.cancel_token.cancel();
        self.topics.iter().for_each(|t| t.value().swim.stop());
        self.msg.close();
        self.conn_inbound.close();
        self.conn_outbound.close();
    }

    /// Test if a broadcast is seen.
    pub fn seen(&self, id: &Uuid) -> bool {
        self.seen.contains(id)
    }

    /// Mark broadcast as seen.
    pub fn saw(&self, id: Uuid) {
        self.seen.insert(id);
    }

    pub fn state(&self) -> State {
        todo!()
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

        t.value()
            .swim
            .send_internal(Broadcast::new_crdt(op).pack()?.serialize()?)
            .await?;

        Ok(true)
    }

    /// Wait for node with corresponding address to join or rejoin.
    pub async fn wait_for(&self, addr: SocketAddr, dur: Duration) -> bool {
        let f = self
            .waiters
            .get_or_insert_with(addr, || Flag::new())
            .value()
            .clone();

        f.timeout(dur).await.is_ok()
    }
}

pub async fn spawn_background(config: Arc<OrkasConfig>) -> Result<Background> {
    let (conn_inbound, inbound_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);
    let (conn_outbound, outbound_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);
    let (msg, msg_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);

    let cancel_token = CancellationToken::new();
    let topics = SkipMap::new().pipe(Arc::new);
    let waiters = SkipMap::new().pipe(Arc::new);
    let seen = SkipSet::new().pipe(Arc::new);

    let ctx = Context {
        actor: Actor::random(),
        msg,
        seen,
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
