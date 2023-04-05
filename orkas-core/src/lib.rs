#![doc = include_str!("../../README.md")]
#![cfg_attr(test, feature(is_sorted))]
#![feature(impl_trait_projections)]
#![feature(type_alias_impl_trait)]
#![feature(default_free_fn)]
#![feature(drain_filter)]
#![feature(try_blocks)]
#![feature(let_chains)]
#![feature(once_cell)]

use std::{net::SocketAddr, time::Duration};

use color_eyre::{eyre::bail, Result};
use limlog::Reader;
use tap::Pipe;
use tracing::info;
use uuid7::uuid7;

pub use crate::model::*;
use crate::{
    tasks::{spawn_background, Background, ContextRef, Swim, TopicEntry},
    util::{CRDTReader, CRDTUpdater},
};

pub mod consts;

mod codec;
mod model;
mod tasks;
mod util;

pub use codec::{adapt, adapt_with_option, SerdeBincodeCodec};

/// The main struct of Orkas.
///
/// This is the entry point of the library. This
/// controls the lifecycle of the background tasks and cannot be cloned. To
/// actually interact with the cluster, you need to create a [`Handle`] instance
/// by calling [`Orkas::handle`]. When dropped, background tasks will be stopped
/// forcefully. To gracefully stop the background tasks, call [`Orkas::stop`].
pub struct Orkas {
    pub background: Background,
}

impl Orkas {
    // TODO: more ergonomic `start_with_*` options
    #[inline]
    pub async fn start(config: OrkasConfig) -> Result<Self> {
        Self {
            background: spawn_background(config.into()).await?,
        }
        .pipe(Ok)
    }

    #[inline]
    pub(crate) fn ctx(&self) -> &ContextRef {
        self.background.ctx()
    }

    /// Stop the background task gracefully. If any task was stopped, this will
    /// return the result of such task with `Some(Vec<Result<()>>)`.
    /// Otherwise, when the task is already stopped, a `None` will be returned.
    #[inline]
    pub async fn stop(&mut self) -> Option<Vec<Result<()>>> {
        self.background.stop().await
    }

    /// Force stop the background task. This will not wait for the background
    /// tasks to be complete.
    #[inline]
    pub fn force_stop(&mut self) {
        self.background.force_stop()
    }

    #[inline]
    pub fn local_addr(&self) -> SocketAddr {
        self.background.addr()
    }

    #[inline]
    /// Get a handle to interact with the cluster.
    pub fn handle(&self) -> Handle {
        Handle {
            ctx: self.ctx().clone(),
            addr: self.local_addr(),
        }
    }
}

/// A cheap-to-clone handle to interact with the cluster. This cannot control
/// the lifecycle of the background tasks. For that, you need to use the
/// [`Orkas`] struct.
#[derive(Clone, Debug)]
pub struct Handle {
    ctx: ContextRef,
    addr: SocketAddr,
}

impl Handle {
    async fn connect_to(&self, addr: SocketAddr) -> Result<()> {
        let (send, recv) = tokio::net::TcpStream::connect(addr) // TODO: fine tuning connection or just use quinn
            .await?
            .into_split()
            .pipe(adapt);

        info!(%addr, "Connected");

        // Send the streams to the background task
        self.ctx.conn_inbound.send(send).await?;
        self.ctx.conn_outbound.send((addr, recv)).await?;

        Ok(())
    }

    #[inline]
    fn make_swim(&self, topic: String) -> Result<Swim> {
        Swim::new(self.addr, self.ctx.clone(), topic)
    }

    /// Update a topic and sync with other nodes in the cluster. This update
    /// will be applied to local cluster state first and distributed to
    /// other nodes in the cluster via swim broadcast.
    ///
    /// A [`CRDTUpdater`] will receive a [`LogList`] and `actor` of the node
    /// upon updating, then optionally returns an [`Op`] to be applied to the
    /// [`LogList`].
    ///
    /// [`Op`]: crdts::list::Op
    pub async fn update<F>(&self, topic: impl AsRef<str>, updater: F) -> Result<bool>
    where
        F: CRDTUpdater,
        F::Error: std::error::Error + Send + Sync + 'static,
    {
        self.ctx.update(topic, updater).await
    }

    /// Read a topic and derive data from it
    #[inline]
    pub fn read<F: CRDTReader>(&self, topic: impl AsRef<str>, func: F) -> Option<F::Return> {
        self.ctx.get_topic(topic).map(|x| func.read(x.crdt()))
    }

    /// Emit an event and broadcast it
    #[inline]
    pub fn log(&self, topic: impl AsRef<str>, log: Log) -> Result<()> {
        self.ctx.log(topic, log)
    }

    #[inline]
    pub fn get_topic(&self, topic: impl AsRef<str>) -> Option<TopicEntry<'_>> {
        self.ctx.get_topic(topic)
    }

    pub fn has_topic(&self, topic: impl AsRef<str>) -> bool {
        self.ctx.has_topic(topic)
    }

    /// Create a new topic in current node
    pub async fn new_topic(&self, topic: impl Into<String>) -> Result<()> {
        let topic = topic.into();

        // Spawn the task and create local record
        let swim = self.make_swim(topic.clone())?.spawn();

        let logs = LogList::new();
        let map = limlog::Topic::builder(topic.clone())?.build().await?;
        let topic_record = Topic { swim, logs, map };

        // Insert the topic record
        self.ctx.insert_topic(topic, topic_record);

        Ok(())
    }

    /// Leave a topic cluster
    pub fn quit_topic(&self, topic: impl Into<String>) -> Result<Option<TopicEntry<'_>>> {
        let topic = topic.into();

        let Some(topic_record) = self.ctx.remove_topic(&topic) else { return Ok(None) };

        topic_record.swim().stop();

        Ok(Some(topic_record))
    }

    /// Join a topic cluster with given address and topic name.
    ///
    /// This will start handshake with correspoding SWIM node. Note that initial
    /// state syncing is not garanteed to be completed within this function.
    pub async fn join_one(&self, topic: impl Into<String>, addr: SocketAddr) -> Result<()> {
        // TODO: use `ToSocketAddrs` instead of `SocketAddr`.
        // TODO: join with multiple initial nodes
        let topic = topic.into();
        if self.has_topic(&topic) {
            return Ok(());
        }

        self.connect_to(addr).await?;

        let mut swim = Swim::new(self.addr, self.ctx.clone(), topic.clone())?;

        let mut rt = self.ctx.swim_runtime(&topic);

        // Start announcing
        swim.announce(addr.into(), &mut rt)?;

        // Spawn the SWIM task
        let swim = swim.spawn();
        let logs = LogList::new();
        let map = limlog::Topic::builder(topic.clone())?.build().await?;
        let topic_record = Topic { swim, logs, map };

        // Insert the topic record
        self.ctx.insert_topic(&topic, topic_record);

        // Send all messages from swim to outbound
        rt.flush().await?;
        drop(rt);

        if !self.ctx.wait_for(addr, Duration::from_secs(5)).await {
            bail!("Timeout waiting for initial join")
        }

        // Now swim is online, we can send the snapshot request
        self.ctx
            .msg
            .send(Envelope {
                addr,
                topic,
                body: Message::RequestSnapshot,
                id: uuid7(),
            })
            .await?;

        Ok(())
    }
}
