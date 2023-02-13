#![doc = include_str!("../README.md")]
#![cfg_attr(test, feature(is_sorted))]
#![feature(type_alias_impl_trait)]
#![feature(default_free_fn)]
#![feature(drain_filter)]
#![feature(try_blocks)]
#![feature(let_chains)]
#![feature(once_cell)]

use std::{net::SocketAddr, time::Duration};

use color_eyre::{eyre::bail, Result};
use tap::Pipe;
use tracing::info;
use uuid7::uuid7;

pub use crate::model::*;
use crate::{
    tasks::{spawn_background, Background, Context, Swim},
    util::{CRDTReader, CRDTUpdater},
};

mod codec;
pub mod consts;
mod model;
mod tasks;
mod util;

pub use codec::{adapt, adapt_with_option, SerdeBincodeCodec};

pub struct Orkas {
    pub background: Background,
}

impl Orkas {
    // TODO: more ergonomic `start_with_*` options
    pub async fn start(config: OrkasConfig) -> Result<Self> {
        Self {
            background: spawn_background(config.into()).await?,
        }
        .pipe(Ok)
    }

    /// Initiate the background task. Some of the functions will not work if
    /// this is not called and will return `Err` immediately.
    pub async fn stop(self) -> Vec<Result<()>> {
        self.background.stop().await
    }

    /// Force stop the background task. This will not wait for the background
    /// tasks to be complete.
    pub fn force_stop(mut self) {
        self.background.force_stop()
    }

    pub(crate) fn ctx(&self) -> &Context {
        self.background.ctx()
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.background.addr()
    }

    fn make_swim(&self, topic: String) -> Result<Swim> {
        Swim::new(self.local_addr(), self.ctx().clone(), topic)
    }

    async fn connect_to(&self, addr: SocketAddr) -> Result<()> {
        let ctx = self.ctx();

        let (send, recv) = tokio::net::TcpStream::connect(addr) // TODO: fine tuning connection or just use quinn
            .await?
            .into_split()
            .pipe(adapt);

        info!(%addr, "Connected");

        // Send the streams to the background task
        ctx.conn_inbound.send(send).await?;
        ctx.conn_outbound.send((addr, recv)).await?;

        Ok(())
    }

    // TODO: use `ToSocketAddrs` instead of `SocketAddr`.
    // TODO: join with multiple initial nodes
    /// Join a topic cluster with given address and topic name.
    ///
    /// This will start handshake with correspoding SWIM node. Note that initial
    /// state syncing is not garanteed to be completed within this function.
    pub async fn join_one(&self, topic: impl Into<String>, addr: SocketAddr) -> Result<()> {
        let topic = topic.into();
        let ctx = self.ctx();

        self.connect_to(addr).await?;

        let mut swim = Swim::new(self.local_addr(), ctx.clone(), topic.clone())?;

        let mut rt = ctx.swim_runtime(&topic);

        // Start announcing
        swim.announce(addr.into(), &mut rt)?;

        // Spawn the SWIM task
        let swim = swim.spawn();
        let logs = LogList::new();
        let topic_record = Topic { swim, logs };

        // Insert the topic record
        ctx.insert_topic(&topic, topic_record);

        // Send all messages from swim to outbound
        rt.flush().await?;
        drop(rt);

        if !ctx.wait_for(addr, Duration::from_secs(5)).await {
            bail!("Timeout waiting for initial join")
        }

        // Now swim is online, we can send the snapshot request
        ctx.msg
            .send(Envelope {
                addr,
                topic,
                body: Message::RequestSnapshot,
                id: uuid7(),
            })
            .await?;

        Ok(())
    }

    /// Create a new topic in current node
    pub fn new_topic(&self, topic: impl Into<String>) -> Result<()> {
        let topic = topic.into();
        let ctx = self.ctx();

        // Spawn the task and create local record
        let swim = self.make_swim(topic.clone())?.spawn();

        let logs = LogList::new();
        let topic_record = Topic { swim, logs };

        // Insert the topic record
        ctx.insert_topic(topic, topic_record);

        Ok(())
    }

    /// Leave a topic cluster
    pub fn quit_topic(&self, topic: impl Into<String>) -> Result<()> {
        let topic = topic.into();
        let ctx = self.ctx();

        let Some(topic_record) = ctx.remove_topic(&topic) else { return Ok(()) };

        topic_record.swim().stop();

        Ok(())
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
        self.ctx().update(topic, updater).await
    }

    /// Read a topic and derive data from it
    pub fn read<F: CRDTReader>(&self, topic: impl AsRef<str>, func: F) -> Option<F::Return> {
        self.ctx().get_topic(topic).map(|x| func.read(&x.crdt()))
    }

    /// Emit an event and broadcast it
    pub fn log(&self, topic: impl AsRef<str>, log: Log) -> Result<()> {
        self.ctx().log(topic, log)
    }
}
