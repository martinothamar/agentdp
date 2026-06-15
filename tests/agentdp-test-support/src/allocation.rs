#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static REPORTING_ENABLED: AtomicBool = AtomicBool::new(false);

pub struct ReportingAllocator<A> {
    inner: A,
    counters: Counters,
}

impl<A> ReportingAllocator<A> {
    pub const fn new(inner: A) -> Self {
        Self {
            inner,
            counters: Counters::new(),
        }
    }

    pub fn begin_report(&self) -> AllocationSnapshot {
        set_reporting(true);
        self.counters.snapshot()
    }

    pub fn report_since(&self, before: AllocationSnapshot) -> AllocationReport {
        let report = self.counters.snapshot().saturating_sub(before);
        set_reporting(false);
        report
    }
}

// SAFETY: This allocator forwards every request to the wrapped allocator with the same pointer,
// layout, and size values, and only updates independent atomic counters around those calls.
unsafe impl<A: GlobalAlloc> GlobalAlloc for ReportingAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller upholds `GlobalAlloc::alloc`; this wrapper forwards unchanged.
        let pointer = unsafe { self.inner.alloc(layout) };
        if !pointer.is_null() && reporting() {
            self.counters.record_alloc(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller upholds `GlobalAlloc::alloc_zeroed`; this wrapper forwards unchanged.
        let pointer = unsafe { self.inner.alloc_zeroed(layout) };
        if !pointer.is_null() && reporting() {
            self.counters.record_alloc(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        if reporting() {
            self.counters.record_dealloc(layout.size());
        }
        // SAFETY: The caller upholds `GlobalAlloc::dealloc`; this wrapper forwards unchanged.
        unsafe { self.inner.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: The caller upholds `GlobalAlloc::realloc`; this wrapper forwards unchanged.
        let new_pointer = unsafe { self.inner.realloc(pointer, layout, new_size) };
        if !new_pointer.is_null() && reporting() {
            self.counters.record_realloc(layout.size(), new_size);
        }
        new_pointer
    }
}

struct Counters {
    allocations: AtomicU64,
    allocated_bytes: AtomicU64,
    deallocations: AtomicU64,
    deallocated_bytes: AtomicU64,
    reallocations: AtomicU64,
}

impl Counters {
    const fn new() -> Self {
        Self {
            allocations: AtomicU64::new(0),
            allocated_bytes: AtomicU64::new(0),
            deallocations: AtomicU64::new(0),
            deallocated_bytes: AtomicU64::new(0),
            reallocations: AtomicU64::new(0),
        }
    }

    fn record_alloc(&self, bytes: usize) {
        self.allocations.fetch_add(1, Ordering::Relaxed);
        self.allocated_bytes
            .fetch_add(u64_from_usize_saturating(bytes), Ordering::Relaxed);
    }

    fn record_dealloc(&self, bytes: usize) {
        self.deallocations.fetch_add(1, Ordering::Relaxed);
        self.deallocated_bytes
            .fetch_add(u64_from_usize_saturating(bytes), Ordering::Relaxed);
    }

    fn record_realloc(&self, old_bytes: usize, new_bytes: usize) {
        self.reallocations.fetch_add(1, Ordering::Relaxed);
        self.allocated_bytes
            .fetch_add(u64_from_usize_saturating(new_bytes), Ordering::Relaxed);
        self.deallocated_bytes
            .fetch_add(u64_from_usize_saturating(old_bytes), Ordering::Relaxed);
    }

    fn snapshot(&self) -> AllocationSnapshot {
        AllocationSnapshot {
            allocations: self.allocations.load(Ordering::Relaxed),
            allocated_bytes: self.allocated_bytes.load(Ordering::Relaxed),
            deallocations: self.deallocations.load(Ordering::Relaxed),
            deallocated_bytes: self.deallocated_bytes.load(Ordering::Relaxed),
            reallocations: self.reallocations.load(Ordering::Relaxed),
        }
    }
}

fn reporting() -> bool {
    REPORTING_ENABLED.load(Ordering::Relaxed)
}

fn set_reporting(reporting: bool) {
    REPORTING_ENABLED.store(reporting, Ordering::Relaxed);
}

#[derive(Clone, Copy, Debug, Default)]
pub struct AllocationReport {
    allocations: u64,
    allocated_bytes: u64,
    deallocations: u64,
    deallocated_bytes: u64,
    reallocations: u64,
}

impl AllocationReport {
    #[must_use]
    pub const fn allocation_calls(self) -> u64 {
        self.allocations + self.reallocations
    }

    #[must_use]
    pub const fn allocated_bytes(self) -> u64 {
        self.allocated_bytes
    }

    #[must_use]
    pub const fn deallocation_calls(self) -> u64 {
        self.deallocations + self.reallocations
    }

    #[must_use]
    pub const fn deallocated_bytes(self) -> u64 {
        self.deallocated_bytes
    }
}

#[derive(Clone, Copy)]
pub struct AllocationSnapshot {
    allocations: u64,
    allocated_bytes: u64,
    deallocations: u64,
    deallocated_bytes: u64,
    reallocations: u64,
}

impl AllocationSnapshot {
    const fn saturating_sub(self, before: Self) -> AllocationReport {
        AllocationReport {
            allocations: self.allocations.saturating_sub(before.allocations),
            allocated_bytes: self.allocated_bytes.saturating_sub(before.allocated_bytes),
            deallocations: self.deallocations.saturating_sub(before.deallocations),
            deallocated_bytes: self.deallocated_bytes.saturating_sub(before.deallocated_bytes),
            reallocations: self.reallocations.saturating_sub(before.reallocations),
        }
    }
}

fn u64_from_usize_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
