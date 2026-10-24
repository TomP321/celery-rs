//! Worker master: consumer, pool supervisor, control, eventi (vedi PLAN.md, fase 5).

pub mod pool;
pub mod wake;

pub use pool::{Config, JobSpec, Pool, PoolError, PoolEvent};
pub use wake::Waker;
