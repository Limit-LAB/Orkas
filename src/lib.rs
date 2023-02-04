#![doc = include_str!("../README.md")]
#![cfg_attr(test, feature(is_sorted))]
#![feature(type_alias_impl_trait)]
#![feature(once_cell)]

use std::net::SocketAddr;

use bincode::DefaultOptions;
use color_eyre::Result;
use foca::{BincodeCodec, Foca};
use rand::{rngs::StdRng, thread_rng, SeedableRng};
use tap::Pipe;

pub use crate::model::*;
use crate::{
    model::{Id, LogList, OrkasConfig, Topic},
    tasks::{spawn_background, spawn_swim, Background, Context, SWIM},
    util::{CRDTReader, CRDTUpdater},
};

mod codec;
mod model;
mod tasks;
mod util;

pub use codec::{adapt, adapt_with_option, SerdeBincodeCodec};

pub struct Orkas {
    pub config: OrkasConfig,
    pub background: Background,
}

impl Orkas {
    // TODO: more ergonomic `start_with_*` options
    pub async fn start(config: OrkasConfig) -> Self {
        Self {
            background: spawn_background(config.bind),
            config,
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
    /// Join a topic cluster with given address and topic name.
    ///
    /// This will start handshake with correspoding SWIM node. Note that initial
    /// state syncing is not garanteed to be completed within this function.
    pub async fn join(&self, topic: String, addr: SocketAddr) -> Result<()> {
        let ctx = self.ctx();
        let (send, recv) = tokio::net::TcpStream::connect(addr)
            .await?
            .into_split()
            .pipe(adapt);

        let mut swim: SWIM = Foca::new(
            Id::from(self.config.bind),
            self.config.foca.clone(),
            StdRng::from_rng(thread_rng())?,
            BincodeCodec(DefaultOptions::new()),
        );

        {
            let mut rt = ctx.swim_runtime(&topic);

            swim.announce(addr.into(), &mut rt)?;

            rt.flush().await?;
        }

        ctx.conn_inbound.send(send).await?;
        ctx.conn_outbound.send((addr, recv)).await?;

        let swim = spawn_swim(topic.clone(), swim, ctx.clone()).await?;
        let logs = LogList::new();
        let topic_record = Topic { swim, logs };

        ctx.topics.insert(topic, topic_record);

        Ok(())
    }

    /// Update a topic and sync with other nodes in the cluster
    pub async fn update<F>(&self, topic: impl Into<String>, func: F) -> Result<bool>
    where
        F: CRDTUpdater,
        F::Error: std::error::Error + Send + Sync + 'static,
    {
        self.ctx().update(topic.into(), func).await
    }

    /// Read a topic and derive data from it
    pub fn read<F: CRDTReader>(&self, topic: impl AsRef<str>, func: F) -> Option<F::Return> {
        self.ctx()
            .topics
            .get(topic.as_ref())
            .map(|x| func.read(&x.value().logs))
    }
}
