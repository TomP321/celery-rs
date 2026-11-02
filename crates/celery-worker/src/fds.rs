//! Registry of Rust-owned descriptors inspected by the master before fork.

use std::collections::{BTreeMap, BTreeSet};
use std::os::fd::RawFd;
use std::sync::{Mutex, PoisonError};

static FDS: crate::per_process::PerProcess<Mutex<BTreeSet<RawFd>>> =
    crate::per_process::PerProcess::new();

fn registry() -> &'static Mutex<BTreeSet<RawFd>> {
    let result =
        FDS.get_or_try_init(|| Ok::<_, std::convert::Infallible>(Mutex::new(BTreeSet::new())));
    match result {
        Ok(registry) => registry,
        Err(never) => match never {},
    }
}

#[derive(Debug)]
pub struct Registration {
    fd: RawFd,
    pid: u32,
}

impl Registration {
    pub fn new(fd: RawFd) -> Self {
        let pid = std::process::id();
        registry()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(fd);
        Self { fd, pid }
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        if self.pid == std::process::id() {
            registry()
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&self.fd);
        }
    }
}

pub fn snapshot() -> BTreeMap<RawFd, (u64, u64)> {
    let registry = registry();
    let fds = registry.lock().unwrap_or_else(PoisonError::into_inner);
    fds.iter()
        .filter_map(|&fd| {
            let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
            // SAFETY: registration keeps the resource alive during fstat; the correctly sized
            // buffer is read only after success.
            if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } != 0 {
                return None;
            }
            // SAFETY: fstat initialized every field of the structure.
            let stat = unsafe { stat.assume_init() };
            Some((fd, (stat.st_dev, stat.st_ino)))
        })
        .collect()
}

#[derive(Debug)]
pub struct Tracked<T> {
    // Field order removes the descriptor from the registry before closing it.
    _registration: Registration,
    value: T,
}

impl<T: std::os::fd::AsRawFd> Tracked<T> {
    pub fn new(value: T) -> Self {
        Self {
            _registration: Registration::new(value.as_raw_fd()),
            value,
        }
    }
}

impl<T> std::ops::Deref for Tracked<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> std::ops::DerefMut for Tracked<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsRawFd;

    #[test]
    fn registration_lives_as_long_as_the_resource() {
        let file = std::fs::File::open("/dev/null").expect("file");
        let fd = file.as_raw_fd();
        let registration = Registration::new(fd);
        assert!(snapshot().contains_key(&fd));
        drop(registration);
        assert!(!snapshot().contains_key(&fd));
        assert!(file.metadata().is_ok());
    }
}
