use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OrchinusConfig {
    pub public_addr: SocketAddr,
    pub foca: foca::Config,
}
