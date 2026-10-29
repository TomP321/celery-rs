//! Worker process pool with a Python zygote and Rust-supervised children.

mod frame;
mod spawn;
mod supervisor;

pub use frame::{Frame, FrameKind};
pub use spawn::{SpawnedChild, Spawner, ZygoteSpawner};
pub use supervisor::{Config, JobSpec, Pool, PoolError, PoolEvent, SIGKILL, SIGTERM, SIGUSR1};
