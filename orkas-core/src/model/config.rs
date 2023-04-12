use std::{io, net::SocketAddr, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{consts::MILLISECOND, Orkas};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OrkasConfig {
    pub bind: SocketAddr,
    pub swim: foca::Config,

    /// When emitting new events, it will be aggregated first and
    /// then sent as a pack. This is to prevent the pack from being too
    /// frequent. But a bigger `send_withhold` also means higher latency.
    pub event_withhold: Duration,

    /// After `send_max_size` events, the pack will be sent even the time limit
    /// is not reached
    pub event_max_size: usize,
}

impl OrkasConfig {
    /// Use simple configuration of Foca
    pub fn simple(bind: SocketAddr) -> Self {
        Self {
            bind,
            swim: foca::Config::simple(),
            event_withhold: 100 * MILLISECOND,
            event_max_size: 1 << 4,
        }
    }
}

impl OrkasConfig {
    pub async fn start(self) -> io::Result<Orkas> {
        Orkas::start(self).await
    }
}
