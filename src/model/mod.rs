mod_use::mod_use![config, data, message];

// TODO: Placeholder for LimitLog
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Log {
    message: String,
    ts: Timestamp,
}
