//! Worker master consumption, process supervision, control and events.

#![deny(unsafe_op_in_unsafe_fn)]
pub mod consumer;
pub mod fds;
pub mod mailbox;
pub mod per_process;
pub mod pool;
pub mod process;
pub mod wake;

pub use consumer::{ConsumerStats, WorkerConsumer};
pub use pool::{Config, JobSpec, Pool, PoolError, PoolEvent};
pub use wake::Waker;
