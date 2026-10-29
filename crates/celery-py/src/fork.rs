//! Prevents inherited asynchronous resources from waking the parent's threads.

use std::mem::ManuallyDrop;
use std::ops::Deref;

pub(crate) struct ForkSafe<T> {
    value: ManuallyDrop<T>,
    pid: u32,
}

impl<T> ForkSafe<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            value: ManuallyDrop::new(value),
            pid: std::process::id(),
        }
    }
}

impl<T> Deref for ForkSafe<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> Drop for ForkSafe<T> {
    fn drop(&mut self) {
        if self.pid == std::process::id() {
            // SAFETY: this is the owning process and the value is dropped only once.
            unsafe { ManuallyDrop::drop(&mut self.value) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct Counter(Arc<AtomicUsize>);
    impl Drop for Counter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn only_owner_runs_destructor() {
        let count = Arc::new(AtomicUsize::new(0));
        drop(ForkSafe::new(Counter(count.clone())));
        assert_eq!(count.load(Ordering::Relaxed), 1);
        let mut orphan = ForkSafe::new(Counter(count.clone()));
        orphan.pid = std::process::id().wrapping_add(1);
        drop(orphan);
        assert_eq!(count.load(Ordering::Relaxed), 1);
    }
}
