//! Worker master: consumer, pool supervisor, control, eventi (vedi PLAN.md, fase 5).

pub mod consumer;
pub mod wake;

pub use consumer::{ConsumerStats, WorkerConsumer};
pub use wake::Waker;
