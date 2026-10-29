//! Registry of Rust-owned descriptors inspected by the master before fork.

use std::collections::{BTreeMap, BTreeSet};
use std::os::fd::RawFd;
use std::sync::{Mutex, PoisonError};

struct Registry {
    pid: u32,
    fds: Mutex<BTreeSet<RawFd>>,
}

// Children create their own registry without touching an inherited mutex.
// Entries live only as long as their registered resources.
static FDS: std::sync::atomic::AtomicPtr<Registry> =
    std::sync::atomic::AtomicPtr::new(std::ptr::null_mut());

fn registry() -> &'static Registry {
    use std::sync::atomic::Ordering;
    let pid = std::process::id();
    loop {
        let current = FDS.load(Ordering::Acquire);
        if !current.is_null() {
            // SAFETY: published registries are never freed.
            let entry = unsafe { &*current };
            if entry.pid == pid {
                return entry;
            }
        }
        let fresh = Box::into_raw(Box::new(Registry {
            pid,
            fds: Mutex::new(BTreeSet::new()),
        }));
        if FDS
            .compare_exchange(current, fresh, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            // SAFETY: the newly published registry remains alive in this process.
            return unsafe { &*fresh };
        }
        // SAFETY: this registry was never published.
        drop(unsafe { Box::from_raw(fresh) });
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
            .fds
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
                .fds
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&self.fd);
        }
    }
}

pub fn snapshot() -> BTreeMap<RawFd, (u64, u64)> {
    let registry = registry();
    let fds = registry.fds.lock().unwrap_or_else(PoisonError::into_inner);
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
