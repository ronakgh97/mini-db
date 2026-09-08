pub mod log;
pub mod protocol;
pub mod wal;
pub mod worker;

pub const MAX_KEY_SIZE: usize = 2 << 20;
pub const MAX_VALUE_SIZE: usize = 24 << 20;

pub static START_TIME: std::sync::OnceLock<chrono::DateTime<chrono::Local>> =
    std::sync::OnceLock::new();

pub fn get_uptime_hrs() -> f64 {
    if let Some(start_time) = START_TIME.get() {
        let now = chrono::Local::now();
        let duration = now.signed_duration_since(*start_time);
        duration.as_seconds_f64() / 3600.0
    } else {
        0.0
    }
}
