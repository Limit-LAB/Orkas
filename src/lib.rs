#![doc = include_str!("../README.md")]
#![cfg_attr(test, feature(is_sorted))]
#![feature(type_alias_impl_trait)]
#![feature(once_cell)]
#![feature(try_blocks)]

use std::{net::SocketAddr, time::Duration};

use color_eyre::{eyre::bail, Result};
use foca::BincodeCodec;
use rand::{rngs::StdRng, thread_rng, SeedableRng};
use tap::Pipe;
use uuid7::uuid7;

pub use crate::model::*;
use crate::{
    codec::bincode_option,
    tasks::{spawn_background, spawn_swim, Background, Context, OrkasBroadcastHandler, SWIM},
    util::{CRDTReader, CRDTUpdater},
};

mod codec;
mod model;
mod tasks;
mod util;

pub use codec::{adapt, adapt_with_option, SerdeBincodeCodec};

pub struct Orkas {
    pub background: Background,
}

impl Orkas {
    // TODO: more ergonomic `start_with_*` options
    pub fn start(config: OrkasConfig) -> Self {
        Self {
            background: spawn_background(config.into()),
        }
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
        &self.background.ctx
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

        let (send, recv) = tokio::net::TcpStream::connect(addr) // TODO: fine tuning connection or just use quinn
            .await?
            .into_split()
            .pipe(adapt);

        // Send the streams to the background task
        ctx.conn_inbound.send(send).await?;
        ctx.conn_outbound.send((addr, recv)).await?;

        let mut swim = SWIM::with_custom_broadcast(
            Id::from(ctx.config.bind),
            ctx.config.foca.clone(),
            StdRng::from_rng(thread_rng())?,
            BincodeCodec(bincode_option()),
            OrkasBroadcastHandler::new(&topic, ctx.clone()),
        );

        let mut rt = ctx.swim_runtime(&topic);

        // Start announcing
        swim.announce(addr.into(), &mut rt)?;

        // Spawn the SWIM task
        let swim = spawn_swim(topic.clone(), swim, ctx.clone());
        let logs = LogList::new();
        let topic_record = Topic { swim, logs };

        // Insert the topic record
        ctx.topics.insert(topic.clone(), topic_record);

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
    pub async fn new_topic(&self, topic: impl Into<String>) -> Result<()> {
        let topic = topic.into();
        let ctx = self.ctx();

        let swim = SWIM::with_custom_broadcast(
            Id::from(ctx.config.bind),
            ctx.config.foca.clone(),
            StdRng::from_rng(thread_rng())?,
            BincodeCodec(bincode_option()),
            OrkasBroadcastHandler::new(&topic, ctx.clone()),
        );

        // Spawn the task and create local record
        let swim = spawn_swim(topic.clone(), swim, ctx.clone());
        let logs = LogList::new();
        let topic_record = Topic { swim, logs };

        // Insert the topic record
        ctx.topics.insert(topic.clone(), topic_record);

        Ok(())
    }

    /// Leave a topic cluster
    pub fn quit_topic(&self, topic: impl Into<String>) -> Result<()> {
        let topic = topic.into();
        let ctx = self.ctx();

        let Some(topic_record) = ctx.topics.remove(&topic) else { return Ok(()) };

        topic_record.value().swim.stop();

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
        self.ctx()
            .topics
            .get(topic.as_ref())
            .map(|x| func.read(&x.value().logs))
    }
}
