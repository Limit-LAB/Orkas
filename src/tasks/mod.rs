mod_use::mod_use![conn, crdt, swim];

use std::{net::SocketAddr, ops::DerefMut, sync::Arc};

use color_eyre::{eyre::bail, Result};
use dashmap::DashMap;
use futures::future::join_all;
use tap::Pipe;
use tokio::{
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    codec::{MessageSink, MessageStream},
    model::{Actor, CRDTMessage, SendTo, State, Topic},
    util::CRDTUpdater,
};

type Inbound = MessageStream<OwnedReadHalf>;
type Outbound = MessageSink<OwnedWriteHalf>;

/// Context of Orkas.
///
/// Contains all the channel sender. This type is relatively cheap to clone and
/// all fields are behind reference count thus all instances targets to same
/// objects.
#[derive(Clone)]
pub(crate) struct Context {
    pub msg: kanal::AsyncSender<SendTo>,
    pub crdt_inbound: kanal::AsyncSender<(String, CRDTMessage)>,
    pub conn_inbound: kanal::AsyncSender<Inbound>,
    pub conn_outbound: kanal::AsyncSender<(SocketAddr, Outbound)>,
    pub topics: Arc<DashMap<String, Topic>>,
    cancel_token: CancellationToken,
    // foca_topics: Arc<DashMap<String, Foca<>>>,
}

pub fn spawn_background(bind: SocketAddr) -> Background {
    let (crdt_inbound, crdt_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);
    let (conn_inbound, inbound_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);
    let (conn_outbound, outbound_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);
    let (msg, msg_rx) = kanal::bounded_async(DEFAULT_CHANNEL_SIZE);

    let cancel_token = CancellationToken::new();
    let crdt_topics = DashMap::new().pipe(Arc::new);

    let ctx = Context {
        msg,
        crdt_inbound,
        conn_inbound,
        conn_outbound,
        topics: crdt_topics,
        cancel_token,
    };

    let inbound = tokio::spawn(inbound_task(inbound_rx, ctx.clone()));
    let outbound = tokio::spawn(outbound_task(msg_rx, outbound_rx, ctx.clone()));
    let listener = tokio::spawn(listener_task(bind, ctx.clone()));
    let crdt = tokio::spawn(crdt_task(crdt_rx, ctx.clone()));
    // let swim = tokio::spawn(swim_task(swim_rx, ctx.clone()));

    Background {
        ctx,
        stopped: false,
        handles: vec![inbound, outbound, listener, crdt],
    }
}

impl Context {
    pub fn close_all(&self) {
        self.cancel_token.cancel();
        self.msg.close();
        self.crdt_inbound.close();
        // self.swim_inbound.close();
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
        let Some(mut t) = self
            .topics
            .get_mut(&topic) else {
                return Ok(false)
            };

        let op = func.update(&mut t.deref_mut().logs, Actor::current())?;
        self.crdt_inbound.send((topic, CRDTMessage::Op(op))).await?;
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
