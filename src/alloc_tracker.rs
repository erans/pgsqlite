use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

/// A simple allocator that tracks allocations for debugging and monitoring purposes
///
/// # Safety
/// This allocator forwards all allocation/deallocation requests to the system allocator
/// while maintaining thread-safe counters for allocation statistics.
pub struct TrackingAllocator;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static DEALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static BYTES_ALLOCATED: AtomicUsize = AtomicUsize::new(0);

// Safety: This implementation is safe because:
// 1. All allocation/deallocation is forwarded to the system allocator
// 2. Counters use atomic operations for thread safety
// 3. No memory is modified directly, only statistical tracking is performed
unsafe impl GlobalAlloc for TrackingAllocator {
    /// Allocate memory using the system allocator and track allocation statistics
    ///
    /// # Safety
    /// This is safe because it directly forwards to System.alloc() with the same layout.
    /// The layout parameter is guaranteed to be valid by the caller (Rust's allocator contract).
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES_ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        // Safety: System.alloc() is safe to call with a valid layout
        unsafe { System.alloc(layout) }
    }

    /// Deallocate memory using the system allocator and track deallocation statistics
    ///
    /// # Safety
    /// This is safe because it directly forwards to System.dealloc() with the same parameters.
    /// The ptr and layout are guaranteed to be valid by the caller (Rust's allocator contract).
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // Safety: System.dealloc() is safe to call with a valid ptr and layout
        unsafe { System.dealloc(ptr, layout) }
    }
}

impl TrackingAllocator {
    /// Reset the allocation counters
    pub fn reset() {
        ALLOCATIONS.store(0, Ordering::Relaxed);
        DEALLOCATIONS.store(0, Ordering::Relaxed);
        BYTES_ALLOCATED.store(0, Ordering::Relaxed);
    }
    
    /// Get the current allocation statistics
    pub fn stats() -> AllocationStats {
        AllocationStats {
            allocations: ALLOCATIONS.load(Ordering::Relaxed),
            deallocations: DEALLOCATIONS.load(Ordering::Relaxed),
            bytes_allocated: BYTES_ALLOCATED.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AllocationStats {
    pub allocations: usize,
    pub deallocations: usize,
    pub bytes_allocated: usize,
}

impl AllocationStats {
    /// Calculate the difference between two snapshots
    pub fn diff(&self, other: &AllocationStats) -> AllocationDiff {
        AllocationDiff {
            allocations: self.allocations.saturating_sub(other.allocations),
            deallocations: self.deallocations.saturating_sub(other.deallocations),
            bytes_allocated: self.bytes_allocated.saturating_sub(other.bytes_allocated),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AllocationDiff {
    pub allocations: usize,
    pub deallocations: usize,
    pub bytes_allocated: usize,
}

impl std::fmt::Display for AllocationDiff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Allocations: {}, Deallocations: {}, Bytes: {}",
            self.allocations, self.deallocations, self.bytes_allocated
        )
    }
}