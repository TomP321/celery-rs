//! Result backend core with a Redis client exposed through PyO3; Python retains backend logic.

#![deny(unsafe_op_in_unsafe_fn)]
pub mod redis;
