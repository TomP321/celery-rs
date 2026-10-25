//! Errori delle schedule, con i testi del riferimento (`celery/schedules.py`).

use crate::ErrorKind;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("empty part")]
    EmptyPart,
    #[error("empty filter")]
    EmptyFilter,
    #[error("negative numbers not supported")]
    NegativeNumber,
    #[error("Invalid end range: {value} > {max}.")]
    EndRange { value: String, max: i64 },
    #[error("Invalid beginning range: {value} < {min}.")]
    BeginRange { value: String, min: i64 },
    #[error("Invalid weekday literal '{literal}'.")]
    InvalidWeekday { literal: String },
    #[error("invalid literal for int() with base 10: '{literal}'")]
    InvalidInt { literal: String },
    #[error("slice step cannot be zero")]
    ZeroStep,
    #[error("min() iterable argument is empty")]
    EmptyMin,
    #[error("max() iterable argument is empty")]
    EmptyMax,
    #[error("unable to rollover, time specification is probably invalid")]
    Rollover,
    /// Set vuoto dopo una modifica a runtime: `list index out of range`.
    #[error("list index out of range")]
    Index,
}

impl Error {
    /// Classe dell'eccezione Python del riferimento che questo errore rappresenta.
    pub fn kind(&self) -> ErrorKind {
        match self {
            Error::EmptyPart | Error::EmptyFilter | Error::NegativeNumber => ErrorKind::Parse,
            Error::EmptyMin | Error::EmptyMax => ErrorKind::Value,
            Error::EndRange { .. }
            | Error::BeginRange { .. }
            | Error::InvalidWeekday { .. }
            | Error::InvalidInt { .. }
            | Error::ZeroStep => ErrorKind::Value,
            Error::Rollover => ErrorKind::Runtime,
            Error::Index => ErrorKind::Index,
        }
    }
}
