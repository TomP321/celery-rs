//! Worker master: consumer, pool supervisor, control, eventi (vedi PLAN.md, fase 5).

pub mod consumer;
pub mod fds;
pub mod mailbox;
pub mod pool;
pub mod wake;

pub use consumer::{ConsumerStats, WorkerConsumer};
pub use pool::{Config, JobSpec, Pool, PoolError, PoolEvent};
pub use wake::Waker;
