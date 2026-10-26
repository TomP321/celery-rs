//! Contatori diagnostici dell'heap Rust; assenti dalla build normale.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use pyo3::prelude::*;

struct CountingAllocator {
    live: AtomicUsize,
    peak: AtomicUsize,
}

impl CountingAllocator {
    const fn new() -> Self {
        Self {
            live: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    fn add(&self, size: usize) {
        let live = self.live.fetch_add(size, Ordering::Relaxed) + size;
        self.peak.fetch_max(live, Ordering::Relaxed);
    }
}

// SAFETY: ogni operazione delega a System con puntatore e layout originali;
// i contatori atomici non allocano né alterano allineamento o durata dei blocchi.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: il layout valido è quello ricevuto dal chiamante.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            self.add(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: System mantiene layout e inizializzazione richiesti.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            self.add(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: puntatore e layout appartengono all'allocazione del chiamante.
        unsafe { System.dealloc(pointer, layout) };
        self.live.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: valgono le stesse precondizioni di GlobalAlloc::realloc.
        let fresh = unsafe { System.realloc(pointer, layout, size) };
        if !fresh.is_null() {
            if size >= layout.size() {
                self.add(size - layout.size());
            } else {
                self.live.fetch_sub(layout.size() - size, Ordering::Relaxed);
            }
        }
        fresh
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

#[pyfunction]
pub(crate) fn _allocator_stats() -> (usize, usize) {
    (
        ALLOCATOR.live.load(Ordering::Relaxed),
        ALLOCATOR.peak.load(Ordering::Relaxed),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_zeroed_reallocated_and_freed_blocks() {
        let allocator = CountingAllocator::new();
        let small = Layout::from_size_align(32, 8).unwrap();
        let large = Layout::from_size_align(96, 8).unwrap();
        // SAFETY: tutti i layout hanno lo stesso allineamento e ogni blocco
        // viene liberato una sola volta con il suo layout corrente.
        unsafe {
            let pointer = allocator.alloc_zeroed(small);
            assert!(!pointer.is_null());
            assert!((0..32).all(|index| *pointer.add(index) == 0));
            assert_eq!(allocator.live.load(Ordering::Relaxed), 32);
            let pointer = allocator.realloc(pointer, small, 96);
            assert!(!pointer.is_null());
            assert_eq!(allocator.live.load(Ordering::Relaxed), 96);
            assert_eq!(allocator.peak.load(Ordering::Relaxed), 96);
            let pointer = allocator.realloc(pointer, large, 32);
            assert!(!pointer.is_null());
            assert_eq!(allocator.live.load(Ordering::Relaxed), 32);
            allocator.dealloc(pointer, small);
        }
        assert_eq!(allocator.live.load(Ordering::Relaxed), 0);
        assert_eq!(allocator.peak.load(Ordering::Relaxed), 96);
    }
}
