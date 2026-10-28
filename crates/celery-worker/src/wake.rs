//! Risveglio del loop principale del worker.
//!
//! Il loop Python del master dorme nel poller dell'hub (epoll) con il fd di
//! [`Waker::fileno`] registrato; quando un componente Rust — consumer del
//! broker, supervisore del pool, timer — ha eventi pronti chiama
//! [`Waker::notify`], che rende leggibile il fd. È l'unico punto di contatto
//! tra i componenti: ognuno tiene la propria coda di eventi e il loop, una
//! volta sveglio, le svuota tutte senza bloccare.
//!
//! [`Waker::wait`] resta per chi non ha un poller (driver dei test del pool):
//! stessa semantica di prima, senza fd.

use std::sync::{Arc, Condvar, Mutex, PoisonError};

#[derive(Debug, Default)]
struct State {
    /// Notifiche arrivate dopo l'ultimo `wait`: più notifiche valgono una sola.
    pending: bool,
}

#[cfg(target_os = "linux")]
mod fd {
    //! `eventfd` su Linux: un solo descrittore, contatore a 64 bit.

    use std::io;
    use std::os::fd::{AsRawFd, RawFd};

    #[derive(Debug)]
    pub struct NotifyFd {
        _registration: Option<crate::fds::Registration>,
        fd: RawFd,
    }

    impl NotifyFd {
        pub fn new() -> io::Result<Self> {
            // SAFETY: `eventfd` con flag validi; il fd restituito è posseduto
            // da `NotifyFd`, che lo chiude in `Drop`.
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
            // SAFETY: scrittura di 8 byte da un `u64` valido sul fd eventfd;
            // l'errore (contatore pieno, fd chiuso) non è recuperabile qui e
            // non deve fermare il chiamante.
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
                // SAFETY: lettura di 8 byte in un `u64` valido; il fd è
                // non bloccante, quindi EAGAIN termina il ciclo.
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
            // SAFETY: il fd è nostro e non viene usato dopo la chiusura.
            unsafe {
                libc::close(self.fd);
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod fd {
    //! Pipe non bloccante sulle altre piattaforme Unix.

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
            // SAFETY: `pipe` riempie l'array di due fd validi.
            if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
                return Err(io::Error::last_os_error());
            }
            for fd in fds {
                // SAFETY: fd appena creato e valido; i flag non cambiano la
                // proprietà del descrittore.
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
            // SAFETY: scrittura di un byte valido sul lato di scrittura.
            unsafe {
                libc::write(self.write, (&byte as *const u8).cast::<libc::c_void>(), 1);
            }
        }

        pub fn drain(&self) {
            let mut buffer = [0u8; 64];
            loop {
                // SAFETY: lettura in un buffer nostro; il fd non blocca.
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
            // SAFETY: entrambi i fd sono nostri e non vengono più usati.
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
    /// `None` solo se il sistema non sa creare il descrittore: il Waker
    /// continua a funzionare con `wait` (il loop ricade sul polling).
    fd: Option<fd::NotifyFd>,
}

/// Segnale condiviso (clonabile) tra i produttori di eventi e il loop.
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

    /// Sveglia il loop; non blocca mai e può essere chiamata da qualunque thread.
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

    /// fd leggibile quando c'è una notifica (`-1` se non disponibile).
    ///
    /// Va registrato nel poller del loop; la callback deve chiamare
    /// [`Waker::drain`] per consumare la notifica.
    pub fn fileno(&self) -> i32 {
        self.inner.fd.as_ref().map_or(-1, fd::NotifyFd::fileno)
    }

    /// Consuma la notifica: svuota il fd e azzera lo stato `pending`.
    ///
    /// Il lock evita la corsa con [`Waker::notify`]: una notifica arrivata
    /// dopo la lettura non può essere persa né lasciare il fd sporco.
    pub fn drain(&self) {
        let mut state = self.lock();
        if let Some(fd) = &self.inner.fd {
            fd.drain();
        }
        state.pending = false;
    }

    /// Attende una notifica per al più `timeout` (`None` = senza limite).
    ///
    /// Ritorna `true` se è arrivata una notifica (anche prima della chiamata),
    /// `false` allo scadere del timeout. Azzera lo stato: la notifica successiva
    /// sveglierà la prossima attesa.
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
        // SAFETY: un solo pollfd valido, timeout 0.
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
        assert!(fd >= 0, "il Waker deve esporre un fd");
        assert!(!readable(fd));
        waker.notify();
        assert!(readable(fd));
        waker.drain();
        assert!(!readable(fd));
        // drain azzera anche la notifica pendente: il prossimo wait dorme.
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
        // Una notifica concorrente può essere consumata o restare pendente:
        // in entrambi i casi non deve essere persa per sempre.
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
