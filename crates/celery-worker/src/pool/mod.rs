//! Pool di processi del worker: zygote + figli Python supervisionati in Rust.
//!
//! Vedi [`supervisor`] per la macchina a stati e [`spawn`] per il protocollo
//! con lo zygote.

mod frame;
mod spawn;
mod supervisor;

pub use frame::{Frame, FrameKind};
pub use spawn::{Spawner, SpawnedChild, ZygoteSpawner};
pub use supervisor::{
    Config, JobSpec, Pool, PoolError, PoolEvent, SIGKILL, SIGTERM, SIGUSR1,
};
