//! Scheduler periodico (vedi PLAN.md, fase 6).
//!
//! Il core Rust copre il parser dei campi crontab e il calcolo del prossimo
//! istante della parte di calendario; fusi orari, DST e `remaining()` restano
//! nel guscio Python (`celery/schedules.py`).

pub mod calendar;
pub mod error;
pub mod parser;

pub use calendar::{CronSpec, FfwdArgs, LastRun};
pub use error::Error;
pub use parser::{CrontabParser, parse};

/// Classe dell'eccezione Python corrispondente a [`Error`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// `celery.schedules.ParseException`
    Parse,
    /// `ValueError`
    Value,
    /// `RuntimeError`
    Runtime,
    /// `IndexError`
    Index,
}
