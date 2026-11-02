//! Lock-free, process-local initialization that never drops inherited resources.

use std::sync::atomic::{AtomicPtr, Ordering};

struct Entry<T> {
    pid: u32,
    value: T,
}

/// Publishes one value per process without acquiring an inherited lock.
/// Published entries intentionally live forever: after fork their destructors may
/// touch locks or runtime threads that no longer exist.
pub struct PerProcess<T> {
    entry: AtomicPtr<Entry<T>>,
}

impl<T: Send + Sync + 'static> Default for PerProcess<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + Sync + 'static> PerProcess<T> {
    pub const fn new() -> Self {
        Self {
            entry: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    pub fn get_or_try_init<E>(&self, init: impl Fn() -> Result<T, E>) -> Result<&'static T, E> {
        let pid = std::process::id();
        loop {
            let current = self.entry.load(Ordering::Acquire);
            if !current.is_null() {
                // SAFETY: published entries are immutable and never freed; acquire observes initialization.
                let entry = unsafe { &*current };
                if entry.pid == pid {
                    return Ok(&entry.value);
                }
            }
            let fresh = Box::into_raw(Box::new(Entry {
                pid,
                value: init()?,
            }));
            if self
                .entry
                .compare_exchange(current, fresh, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // SAFETY: the successful publication keeps this initialized entry alive forever.
                return Ok(unsafe { &(*fresh).value });
            }
            // SAFETY: failed publication leaves fresh exclusively owned by this thread.
            drop(unsafe { Box::from_raw(fresh) });
        }
    }

    /// Abandons the inherited entry without running its destructor.
    pub fn reset(&self) {
        self.entry.swap(std::ptr::null_mut(), Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn concurrent_initialization_publishes_one_value() {
        let cell = Arc::new(PerProcess::new());
        let threads: Vec<_> = (0..8)
            .map(|i| {
                let cell = Arc::clone(&cell);
                std::thread::spawn(move || cell.get_or_try_init(|| Ok::<_, ()>(i)).unwrap())
            })
            .collect();
        let values: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        assert!(values.iter().all(|v| std::ptr::eq(*v, values[0])));
    }

    #[test]
    fn errors_can_be_retried_and_reset_preserves_old_references() {
        let cell = PerProcess::new();
        assert_eq!(
            cell.get_or_try_init(|| Err::<u32, _>("failed")),
            Err("failed")
        );
        let old = cell.get_or_try_init(|| Ok::<_, ()>(1)).unwrap();
        cell.reset();
        assert_eq!(*cell.get_or_try_init(|| Ok::<_, ()>(2)).unwrap(), 2);
        assert_eq!(*old, 1);
    }

    #[test]
    fn inherited_entry_is_replaced_without_locking_it() {
        // Simulates a forked PID while holding the old value's mutex.
        let cell = PerProcess::new();
        let old = cell
            .get_or_try_init(|| Ok::<_, ()>(std::sync::Mutex::new(1)))
            .unwrap();
        let _guard = old.lock().unwrap();
        let entry = cell.entry.load(Ordering::Acquire);
        // SAFETY: this test is the sole user of the cell and changes only its PID tag.
        unsafe {
            (*entry).pid = std::process::id().wrapping_add(1);
        }
        let fresh = cell
            .get_or_try_init(|| Ok::<_, ()>(std::sync::Mutex::new(2)))
            .unwrap();
        assert_eq!(*fresh.lock().unwrap(), 2);
    }
}
