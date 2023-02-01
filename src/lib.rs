#![doc = include_str!("../README.md")]
#![cfg_attr(test, feature(is_sorted))]
#![feature(type_alias_impl_trait)]
#![feature(once_cell)]

use bincode::DefaultOptions;
use color_eyre::Result;
use foca::{BincodeCodec, Foca, NoCustomBroadcast};
use rand::rngs::ThreadRng;

mod_use::mod_use![background, model, util, codec];

pub struct Orkas {
    pub foca: Foca<Id, BincodeCodec<DefaultOptions>, ThreadRng, NoCustomBroadcast>,
    pub config: OrkasConfig,
    pub background: Option<BackgroundHandle>,
}

impl Orkas {
    pub fn new(config: OrkasConfig) -> Result<Self> {
        // let (server_conf, cert) = config.configure_server()?;
        let (bg, crdt, swim) = spawn_connections(config.bind);
        let this = Self {
            foca: Foca::new(
                config.bind.into(),
                config.foca.clone(),
                ThreadRng::default(),
                BincodeCodec(DefaultOptions::new()),
            ),
            config,
            background: None,
        };

        Ok(this)
    }

    pub async fn main_loop(self) -> Result<()> {
        Ok(())
    }
}
