use std::{net::SocketAddr, time::Duration};

use color_eyre::Result;
use quinn::ServerConfig;
use serde::{Deserialize, Serialize};
use tap::Pipe;

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
    pub(crate) fn configure_server(&self) -> Result<(ServerConfig, Vec<u8>)> {
        let cert = rcgen::generate_simple_self_signed(vec![])?;
        let cert_der = cert.serialize_der()?;
        let priv_key = cert.serialize_private_key_der().pipe(rustls::PrivateKey);
        let cert_chain = vec![rustls::Certificate(cert_der.clone())];

        let server_config = ServerConfig::with_single_cert(cert_chain, priv_key)?;

        Ok((server_config, cert_der))
    }

    pub async fn start(self) -> Result<Orkas> {
        Orkas::start(self).await
    }
}
