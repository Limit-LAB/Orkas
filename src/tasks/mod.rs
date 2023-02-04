mod_use::mod_use![conn, swim];

use std::{net::SocketAddr, sync::Arc};

use color_eyre::{eyre::bail, Result};
use crossbeam_skiplist::SkipMap;
use futures::future::join_all;
use tap::Pipe;
use tokio::{
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    codec::{MessageSink, MessageStream},
    model::{Actor, Envelope, State, Topic},
    util::CRDTUpdater,
    Broadcast, InternalMessage,
};

type Inbound = MessageStream<OwnedReadHalf>;
type Outbound = MessageSink<OwnedWriteHalf>;

/// Context of Orkas.
///
/// Contains all the channel sender. This type is relatively cheap to clone and
/// all fields are behind reference count thus all instances targets to same
/// objects.
#[derive(Clone)]
pub struct Context {
    pub msg: kanal::AsyncSender<Envelope>,
    pub conn_inbound: kanal::AsyncSender<Inbound>,
    pub conn_outbound: kanal::AsyncSender<(SocketAddr, Outbound)>,
    pub topics: Arc<SkipMap<String, Topic>>,
    cancel_token: CancellationToken,
}

pub fn spawn_background(bind: SocketAddr) -> Background {
    let (conn_inbound, inbound_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);
    let (conn_outbound, outbound_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);
    let (msg, msg_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);

    let cancel_token = CancellationToken::new();
    let topics = SkipMap::new().pipe(Arc::new);

    let ctx = Context {
        msg,
        conn_inbound,
        conn_outbound,
        topics,
        cancel_token,
    };

    let inbound = tokio::spawn(inbound_task(inbound_rx, ctx.clone()));
    let outbound = tokio::spawn(outbound_task(msg_rx, outbound_rx, ctx.clone()));
    let listener = tokio::spawn(listener_task(bind, ctx.clone()));

    Background {
        ctx,
        stopped: false,
        handles: vec![inbound, outbound, listener],
    }
}

impl Context {
    pub fn close_all(&self) {
        self.cancel_token.cancel();
        self.msg.close();
        self.conn_inbound.close();
        self.conn_outbound.close();
    }

    pub fn state(&self) -> State {
        todo!()
    }

    pub async fn update<F>(&self, topic: String, func: F) -> Result<bool>
    where
        F: CRDTUpdater,
        F::Error: Send + Sync + 'static,
    {
        let Some(t) = self.topics.get(&topic) else { return Ok(false) };

        let op = func.update(&t.value().logs, Actor::current())?;

        t.value()
            .swim
            .send_internal(InternalMessage::Broadcast(Broadcast::CrdtOp(op)))
            .await?;

        Ok(true)
    }
}

pub struct Background {
    pub(crate) ctx: Context,
    handles: Vec<JoinHandle<Result<()>>>,
    stopped: bool,
}

impl Drop for Background {
    fn drop(&mut self) {
        if !self.stopped {
            self.force_stop();
        }
    }
}

impl Background {
    /// Check if all background tasks are still running.
    pub fn is_running(&self) -> bool {
        !(self.ctx.cancel_token.is_cancelled() || self.handles.iter().any(|x| x.is_finished()))
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
        self.stopped = true;
        self.ctx.close_all();
        self.handles.iter().for_each(|x| x.abort());
    }

    /// Wait for the background tasks to finish. Use with
    /// [`BackgroundHandle::cancel`] to gracefully shutdown the background
    /// tasks.
    pub async fn stop(mut self) -> Vec<Result<()>> {
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
            .collect()
    }
}
