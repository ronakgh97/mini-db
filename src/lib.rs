pub mod log;
pub mod manager;
pub mod protocol;
pub mod wal;
pub mod worker;

pub const MAX_KEY_SIZE: usize = 2 << 20;
pub const MAX_VALUE_SIZE: usize = 24 << 20;
pub const MAX_DB_NAME_LEN: usize = 64;
pub const DEFAULT_DB_NAME: &str = "default";

pub static START_TIME: std::sync::OnceLock<chrono::DateTime<chrono::Local>> =
    std::sync::OnceLock::new();

#[inline(always)]
pub fn get_uptime_hrs() -> f64 {
    if let Some(start_time) = START_TIME.get() {
        let now = chrono::Local::now();
        let duration = now.signed_duration_since(*start_time);
        duration.as_seconds_f64() / 3600.0
    } else {
        0.0
    }
}

#[inline(always)]
pub fn fmt_bytes(bytes: usize) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.3} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.3} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.3} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.3} B", bytes)
    }
}
