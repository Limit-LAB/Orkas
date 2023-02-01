use std::net::SocketAddr;

use color_eyre::Result;
use quinn::ServerConfig;
use serde::{Deserialize, Serialize};
use tap::Pipe;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OrkasConfig {
    pub bind: SocketAddr,
    pub servername: String,
    pub foca: foca::Config,
}

impl OrkasConfig {
    pub fn configure_server(&self) -> Result<(ServerConfig, Vec<u8>)> {
        let cert = rcgen::generate_simple_self_signed(vec![self.servername.clone()])?;
        let cert_der = cert.serialize_der()?;
        let priv_key = cert.serialize_private_key_der().pipe(rustls::PrivateKey);
        let cert_chain = vec![rustls::Certificate(cert_der.clone())];

        let server_config = ServerConfig::with_single_cert(cert_chain, priv_key)?;

        Ok((server_config, cert_der))
    }
}
