//! Crontab field parsing and calendar scheduling. Python retains time zones, DST and remaining().

pub mod calendar;
pub mod error;
pub mod parser;

pub use calendar::{CronSpec, FfwdArgs, LastRun};
pub use error::Error;
pub use parser::{CrontabParser, parse};

/// Python exception class corresponding to [`Error`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Celery schedule parse exception.
    Parse,
    /// Python value error.
    Value,
    /// Python runtime error.
    Runtime,
    /// Python index error.
    Index,
}
