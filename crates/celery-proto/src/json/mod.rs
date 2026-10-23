//! JSON compatibile con CPython/kombu: scrittura byte-identica e parser a eventi.

pub mod read;
pub mod write;

pub use read::{Event, JsonStr, ParseError, Reader};
pub use write::{write_float, write_int, write_str, write_str_code_points};
