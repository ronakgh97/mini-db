use clap::ValueEnum;
use colored::{ColoredString, Colorize};

/// Global log level setter for the server
pub static LOG_LEVEL: std::sync::OnceLock<Level> = std::sync::OnceLock::new();

/// Log levels for the server
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum Level {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
}

#[inline(always)]
fn should_log(level: Level) -> bool {
    level >= *LOG_LEVEL.get_or_init(|| Level::Info)
}

#[inline(always)]
pub fn log(level: Level, msg: ColoredString) {
    if !should_log(level) {
        return;
    }

    let now = chrono::Local::now();

    let level_str = match level {
        Level::Trace => "TRACE".dimmed(),
        Level::Debug => "DEBUG".blue().bold(),
        Level::Info => "INFO".green().bold(),
        Level::Warn => "WARN".yellow().bold(),
        Level::Error => "ERROR".red().bold(),
    };

    // log to terminal
    println!("[{}][{}] {}", now.format("%H:%M:%S"), level_str, msg);
}

#[macro_export]
macro_rules! trace {
    ($($arg:tt)*) => {
        {
            use colored::Colorize;
            $crate::log::log($crate::log::Level::Trace, format!($($arg)*).dimmed())
        }
    };
}

#[macro_export]
macro_rules! debug {
    ($($arg:tt)*) => {
        {
            use colored::Colorize;
            $crate::log::log($crate::log::Level::Debug, format!($($arg)*).blue())
        }
    };
}

#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => {
        {
            use colored::Colorize;
            $crate::log::log($crate::log::Level::Info, format!($($arg)*).green())
        }
    };
}

#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => {
        {
            use colored::Colorize;
            $crate::log::log($crate::log::Level::Warn, format!($($arg)*).yellow())
        }
    };
}

#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => {
        {
            use colored::Colorize;
            $crate::log::log($crate::log::Level::Error, format!($($arg)*).red())
        }
    };
}
