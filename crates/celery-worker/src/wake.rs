//! Risveglio del loop principale del worker.
//!
//! Il loop Python del master dorme in [`Waker::wait`] (GIL rilasciato) finché un
//! componente Rust — consumer del broker, supervisore del pool, timer — non ha
//! eventi pronti e chiama [`Waker::notify`]. È l'unico punto di contatto tra i
//! componenti: ognuno tiene la propria coda di eventi e il loop, una volta
//! sveglio, le svuota tutte senza bloccare.

use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

#[derive(Debug, Default)]
struct State {
    /// Notifiche arrivate dopo l'ultimo `wait`: più notifiche valgono una sola.
    pending: bool,
}

/// Segnale condiviso (clonabile) tra i produttori di eventi e il loop.
#[derive(Debug, Clone, Default)]
pub struct Waker {
    inner: Arc<(Mutex<State>, Condvar)>,
}

impl Waker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Sveglia il loop; non blocca mai e può essere chiamata da qualunque thread.
    pub fn notify(&self) {
        let (lock, cvar) = &*self.inner;
        let mut state = lock.lock().unwrap_or_else(PoisonError::into_inner);
        if !state.pending {
            state.pending = true;
            cvar.notify_one();
        }
    }

    /// Attende una notifica per al più `timeout` (`None` = senza limite).
    ///
    /// Ritorna `true` se è arrivata una notifica (anche prima della chiamata),
    /// `false` allo scadere del timeout. Azzera lo stato: la notifica successiva
    /// sveglierà la prossima attesa.
    pub fn wait(&self, timeout: Option<Duration>) -> bool {
        let (lock, cvar) = &*self.inner;
        let mut state = lock.lock().unwrap_or_else(PoisonError::into_inner);
        let deadline = timeout.map(|t| Instant::now() + t);
        while !state.pending {
            match deadline {
                None => {
                    state = cvar.wait(state).unwrap_or_else(PoisonError::into_inner);
                }
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return false;
                    }
                    state = cvar
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
}
