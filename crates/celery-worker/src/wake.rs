//! Coalesced notifications wake the Python hub through a readable descriptor.
//! Components retain their own event queues; wait also supports callers without a poller.

use std::sync::{Arc, Condvar, Mutex, PoisonError};

#[derive(Debug, Default)]
struct State {
    // Coalesces notifications received since the last wait.
    pending: bool,
}

#[cfg(target_os = "linux")]
mod fd {
    use std::fs::File;
    use std::io::{self, Read, Write};
    use std::os::fd::{AsRawFd, FromRawFd, RawFd};

    #[derive(Debug)]
    pub struct NotifyFd {
        // Unregister before File closes the descriptor.
        _registration: crate::fds::Registration,
        file: File,
    }

    impl NotifyFd {
        pub fn new() -> io::Result<Self> {
            // SAFETY: eventfd receives valid flags. On success its fresh descriptor
            // is transferred exactly once to File, which owns it until drop.
            let file = unsafe {
                let fd = libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK);
                if fd < 0 {
                    return Err(io::Error::last_os_error());
                }
                File::from_raw_fd(fd)
            };
            Ok(Self {
                _registration: crate::fds::Registration::new(file.as_raw_fd()),
                file,
            })
        }

        pub fn signal(&self) {
            let _ = (&self.file).write(&1u64.to_ne_bytes());
        }

        pub fn drain(&self) {
            let mut value = [0; 8];
            while matches!((&self.file).read(&mut value), Ok(n) if n > 0) {}
        }

        pub fn fileno(&self) -> RawFd {
            self.file.as_raw_fd()
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod fd {
    use std::io::{self, PipeReader, PipeWriter, Read, Write};
    use std::os::fd::{AsRawFd, RawFd};
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Debug)]
    pub struct NotifyFd {
        read: PipeReader,
        write: PipeWriter,
        signaled: AtomicBool,
    }

    impl NotifyFd {
        pub fn new() -> io::Result<Self> {
            let (read, write) = io::pipe()?;
            Ok(Self {
                read,
                write,
                signaled: AtomicBool::new(false),
            })
        }

        pub fn signal(&self) {
            // The outer state lock serializes signal/drain. At most one byte is
            // outstanding, so the blocking std pipe never fills or reads an empty pipe.
            if !self.signaled.load(Ordering::Relaxed) && (&self.write).write_all(&[1]).is_ok() {
                self.signaled.store(true, Ordering::Relaxed);
            }
        }

        pub fn drain(&self) {
            if self.signaled.load(Ordering::Relaxed) && (&self.read).read_exact(&mut [0]).is_ok() {
                self.signaled.store(false, Ordering::Relaxed);
            }
        }

        pub fn fileno(&self) -> RawFd {
            self.read.as_raw_fd()
        }
    }
}

struct Inner {
    state: Mutex<State>,
    cvar: Condvar,
    // Descriptor creation failure falls back to wait and polling.
    fd: Option<fd::NotifyFd>,
}

/// Shared, cloneable notification signal for event producers and the master loop.
#[derive(Clone)]
pub struct Waker {
    inner: Arc<Inner>,
}

impl Default for Waker {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Waker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pending = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pending;
        f.debug_struct("Waker")
            .field("pending", &pending)
            .field("fileno", &self.fileno())
            .finish()
    }
}

impl Waker {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State::default()),
                cvar: Condvar::new(),
                fd: fd::NotifyFd::new().ok(),
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Wakes the loop without blocking from any thread.
    pub fn notify(&self) {
        let mut state = self.lock();
        if !state.pending {
            state.pending = true;
            if let Some(fd) = &self.inner.fd {
                fd.signal();
            }
            self.inner.cvar.notify_one();
        }
    }

    /// Returns the notification descriptor, or -1 when unavailable. Register it with
    /// the loop poller and call [`Waker::drain`] from its callback.
    pub fn fileno(&self) -> i32 {
        self.inner.fd.as_ref().map_or(-1, fd::NotifyFd::fileno)
    }

    /// Consumes pending notifications and drains the descriptor under a lock, preventing
    /// concurrent notifications from being lost or leaving the descriptor readable.
    pub fn drain(&self) {
        let mut state = self.lock();
        if let Some(fd) = &self.inner.fd {
            fd.drain();
        }
        state.pending = false;
    }

    /// Waits for a notification up to the timeout; None waits indefinitely. Returns
    /// false on timeout and clears pending state so the next notification wakes a new wait.
    pub fn wait(&self, timeout: Option<std::time::Duration>) -> bool {
        use std::time::Instant;

        let mut state = self.lock();
        let deadline = timeout.map(|t| Instant::now() + t);
        while !state.pending {
            match deadline {
                None => {
                    state = self
                        .inner
                        .cvar
                        .wait(state)
                        .unwrap_or_else(PoisonError::into_inner);
                }
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return false;
                    }
                    state = self
                        .inner
                        .cvar
                        .wait_timeout(state, deadline - now)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0;
                }
            }
        }
        state.pending = false;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::{Duration, Instant};

    fn readable(fd: i32) -> bool {
        let mut pollfd = libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: poll receives one valid pollfd and a zero timeout.
        let ready = unsafe { libc::poll(&mut pollfd, 1, 0) };
        ready > 0 && pollfd.revents & libc::POLLIN != 0
    }

    #[test]
    fn notify_before_wait_is_not_lost() {
        let waker = Waker::new();
        waker.notify();
        assert!(waker.wait(Some(Duration::from_millis(0))));
        assert!(!waker.wait(Some(Duration::from_millis(10))));
    }

    #[test]
    fn notifications_coalesce() {
        let waker = Waker::new();
        waker.notify();
        waker.notify();
        assert!(waker.wait(Some(Duration::ZERO)));
        assert!(!waker.wait(Some(Duration::ZERO)));
    }

    #[test]
    fn wakes_from_other_thread() {
        let waker = Waker::new();
        let other = waker.clone();
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            other.notify();
        });
        let start = Instant::now();
        assert!(waker.wait(Some(Duration::from_secs(5))));
        assert!(start.elapsed() < Duration::from_secs(5));
        handle.join().expect("thread");
    }

    #[test]
    fn timeout_expires() {
        let waker = Waker::new();
        let start = Instant::now();
        assert!(!waker.wait(Some(Duration::from_millis(30))));
        assert!(start.elapsed() >= Duration::from_millis(30));
    }

    #[test]
    fn fileno_becomes_readable_and_drain_clears_it() {
        let waker = Waker::new();
        let fd = waker.fileno();
        assert!(fd >= 0, "waker must expose a descriptor");
        assert!(!readable(fd));
        waker.notify();
        assert!(readable(fd));
        waker.drain();
        assert!(!readable(fd));

        assert!(!waker.wait(Some(Duration::from_millis(10))));
    }

    #[test]
    fn drain_after_notify_during_drain_is_not_lost() {
        let waker = Waker::new();
        waker.notify();
        let other = waker.clone();
        let handle = thread::spawn(move || {
            other.notify();
        });
        waker.drain();
        handle.join().expect("thread");
        // Concurrent notifications may be consumed or remain pending, but must never be lost.
        if !waker.wait(Some(Duration::from_millis(50))) {
            waker.notify();
            assert!(waker.wait(Some(Duration::from_millis(50))));
        }
    }

    #[test]
    fn clones_share_the_same_fd() {
        let waker = Waker::new();
        let clone = waker.clone();
        assert_eq!(waker.fileno(), clone.fileno());
        clone.notify();
        assert!(readable(waker.fileno()));
        waker.drain();
    }
}
