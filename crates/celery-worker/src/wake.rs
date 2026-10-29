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
    //! Linux eventfd notifications using one descriptor and a 64-bit counter.

    use std::io;
    use std::os::fd::{AsRawFd, RawFd};

    #[derive(Debug)]
    pub struct NotifyFd {
        _registration: Option<crate::fds::Registration>,
        fd: RawFd,
    }

    impl NotifyFd {
        pub fn new() -> io::Result<Self> {
            // SAFETY: eventfd receives valid flags; NotifyFd owns and closes the returned descriptor.
            let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
            if fd < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(Self {
                    _registration: Some(crate::fds::Registration::new(fd)),
                    fd,
                })
            }
        }

        pub fn signal(&self) {
            let value: u64 = 1;
            // SAFETY: the source is a valid eight-byte u64 and fd is an eventfd. Write errors
            // need no recovery here and must not stop the caller.
            unsafe {
                libc::write(
                    self.fd,
                    (&value as *const u64).cast::<libc::c_void>(),
                    std::mem::size_of::<u64>(),
                );
            }
        }

        pub fn drain(&self) {
            let mut value: u64 = 0;
            loop {
                // SAFETY: the destination is a valid eight-byte u64; the descriptor is nonblocking,
                // so EAGAIN ends the drain loop.
                let read = unsafe {
                    libc::read(
                        self.fd,
                        (&mut value as *mut u64).cast::<libc::c_void>(),
                        std::mem::size_of::<u64>(),
                    )
                };
                if read <= 0 {
                    break;
                }
            }
        }

        pub fn fileno(&self) -> RawFd {
            self.fd.as_raw_fd()
        }
    }

    impl Drop for NotifyFd {
        fn drop(&mut self) {
            self._registration.take();
            // SAFETY: this object owns the descriptor and never uses it after closure.
            unsafe {
                libc::close(self.fd);
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod fd {
    //! Nonblocking pipe notifications on other Unix platforms.

    use std::io;
    use std::os::fd::RawFd;

    #[derive(Debug)]
    pub struct NotifyFd {
        read: RawFd,
        write: RawFd,
    }

    impl NotifyFd {
        pub fn new() -> io::Result<Self> {
            let mut fds = [0 as RawFd; 2];
            // SAFETY: pipe initializes the two-element descriptor array on success.
            if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
                return Err(io::Error::last_os_error());
            }
            for fd in fds {
                // SAFETY: the descriptor is newly created and valid; flags do not change its ownership.
                unsafe {
                    libc::fcntl(fd, libc::F_SETFL, libc::O_NONBLOCK);
                    libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
                }
            }
            Ok(Self {
                read: fds[0],
                write: fds[1],
            })
        }

        pub fn signal(&self) {
            let byte = 1u8;
            // SAFETY: the source byte is valid and fd is the pipe's write endpoint.
            unsafe {
                libc::write(self.write, (&byte as *const u8).cast::<libc::c_void>(), 1);
            }
        }

        pub fn drain(&self) {
            let mut buffer = [0u8; 64];
            loop {
                // SAFETY: the destination buffer is owned here and the descriptor is nonblocking.
                let read = unsafe {
                    libc::read(
                        self.read,
                        buffer.as_mut_ptr().cast::<libc::c_void>(),
                        buffer.len(),
                    )
                };
                if read <= 0 {
                    break;
                }
            }
        }

        pub fn fileno(&self) -> RawFd {
            self.read
        }
    }

    impl Drop for NotifyFd {
        fn drop(&mut self) {
            // SAFETY: both descriptors are owned here and never used after closure.
            unsafe {
                libc::close(self.read);
                libc::close(self.write);
            }
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
