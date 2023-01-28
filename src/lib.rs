#![doc = include_str!("../README.md")]
#![cfg_attr(test, feature(is_sorted))]

use std::net::SocketAddr;

use bincode::DefaultOptions;
use foca::{BincodeCodec, Foca, NoCustomBroadcast};
use rand::rngs::ThreadRng;
use serde::{Deserialize, Serialize};

mod_use::mod_use![config, codec, message, logs, data];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Id {
    addr: SocketAddr,
    rev: u64,
}

impl foca::Identity for Id {
    fn renew(&self) -> Option<Self> {
        let Self { addr, rev } = self;

        Some(Self {
            addr: *addr,
            rev: rev.wrapping_add(1),
        })
    }

    fn has_same_prefix(&self, other: &Self) -> bool {
        self.addr == other.addr
    }
}

impl From<SocketAddr> for Id {
    fn from(addr: SocketAddr) -> Self {
        Self { addr, rev: 0 }
    }
}

pub struct Orchinus {
    // config: config::OrchinusConfig,
    foca: Foca<Id, BincodeCodec<DefaultOptions>, ThreadRng, NoCustomBroadcast>,
    quic: quinn::Endpoint,
}

impl Orchinus {
    pub fn new(config: config::OrchinusConfig) -> Self {
        let mut this = Self {
            foca: Foca::new(
                config.public_addr.into(),
                config.foca,
                ThreadRng::default(),
                BincodeCodec(DefaultOptions::new()),
            ),
            quic: todo!(),
            // config,
        };

        this
    }
}
