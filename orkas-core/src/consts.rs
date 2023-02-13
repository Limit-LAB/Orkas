use std::time::Duration;

// 256 items
pub const DEFAULT_CHANNEL_SIZE: usize = 1 << 8;

// 4k bytes buffer
pub const DEFAULT_BUFFER_SIZE: usize = 1 << 12;

// Time unit
pub const MILLISECOND: Duration = Duration::from_millis(1);
pub const SECOND: Duration = Duration::from_secs(1);
pub const MINUTE: Duration = Duration::from_secs(60);
