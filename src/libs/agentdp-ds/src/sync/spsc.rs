#![allow(unsafe_code)]

use std::mem::MaybeUninit;

use sync::UnsafeCell;
use sync::atomic::{AtomicBool, AtomicU64, Ordering};
use sync::rc::Arc;

#[cfg(feature = "loom")]
mod sync {
    pub(crate) use loom::cell::UnsafeCell;

    pub(crate) mod atomic {
        pub(crate) use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    }

    pub(crate) mod rc {
        pub(crate) use loom::sync::Arc;
    }
}

#[cfg(not(feature = "loom"))]
mod sync {
    #[derive(Debug)]
    pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

    pub(crate) mod atomic {
        pub(crate) use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    }

    pub(crate) mod rc {
        pub(crate) use std::sync::Arc;
    }

    impl<T> UnsafeCell<T> {
        pub(crate) const fn new(value: T) -> Self {
            Self(std::cell::UnsafeCell::new(value))
        }

        pub(crate) fn with_mut<R>(&self, access: impl FnOnce(*mut T) -> R) -> R {
            access(self.0.get())
        }
    }
}

#[derive(Debug)]
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
    cached_head: u64,
}

#[derive(Debug)]
pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
    cached_tail: u64,
}

#[derive(Debug)]
struct Inner<T> {
    slots: Box<[Slot<T>]>,
    capacity: u64,
    mask: u64,
    head: CachePadded<AtomicU64>,
    tail: CachePadded<AtomicU64>,
    sender_alive: AtomicBool,
    receiver_alive: AtomicBool,
}

#[repr(align(128))]
#[derive(Debug)]
struct CachePadded<T>(T);

#[derive(Debug)]
struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
}

// SAFETY: SPSC discipline guarantees that the producer writes a slot before publishing tail and
// the consumer reads it only after observing tail. No two threads access the same initialized slot
// mutably at the same time.
unsafe impl<T: Send> Sync for Slot<T> {}

#[derive(Debug, PartialEq, Eq)]
pub enum TrySendError<T> {
    Full(T),
    Disconnected(T),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    Empty,
    Disconnected,
}

#[must_use]
pub fn bounded<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let capacity = capacity.max(1);
    let physical_capacity = capacity.next_power_of_two();
    let slots = std::iter::repeat_with(|| Slot {
        value: UnsafeCell::new(MaybeUninit::uninit()),
    })
    .take(physical_capacity)
    .collect::<Vec<_>>()
    .into_boxed_slice();
    let inner = Arc::new(Inner {
        slots,
        capacity: capacity as u64,
        mask: (physical_capacity - 1) as u64,
        head: CachePadded(AtomicU64::new(0)),
        tail: CachePadded(AtomicU64::new(0)),
        sender_alive: AtomicBool::new(true),
        receiver_alive: AtomicBool::new(true),
    });
    (
        Sender {
            inner: Arc::clone(&inner),
            cached_head: 0,
        },
        Receiver { inner, cached_tail: 0 },
    )
}

impl<T> Sender<T> {
    /// Attempts to enqueue a value without blocking.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] with the original value when the bounded queue is full.
    /// Returns [`TrySendError::Disconnected`] with the original value when the receiver was dropped.
    pub fn try_send(&mut self, value: T) -> Result<(), TrySendError<T>> {
        if !self.inner.receiver_alive.load(Ordering::Acquire) {
            return Err(TrySendError::Disconnected(value));
        }

        let tail = self.inner.tail.0.load(Ordering::Relaxed);
        if tail.wrapping_sub(self.cached_head) == self.inner.capacity {
            self.cached_head = self.inner.head.0.load(Ordering::Acquire);
            if tail.wrapping_sub(self.cached_head) == self.inner.capacity {
                return Err(TrySendError::Full(value));
            }
        }

        let index = ring_index(tail, self.inner.mask);
        // SAFETY: the slot is not visible to the consumer until the release-store to tail below.
        // The full check above guarantees this slot is outside the active [head, tail) range.
        self.inner.slots[index].value.with_mut(|slot| unsafe {
            (*slot).write(value);
        });
        self.inner.tail.0.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.inner.sender_alive.store(false, Ordering::Release);
    }
}

impl<T> Receiver<T> {
    /// Attempts to dequeue a value without blocking.
    ///
    /// # Errors
    ///
    /// Returns [`TryRecvError::Empty`] when no value is currently queued and the sender is still alive.
    /// Returns [`TryRecvError::Disconnected`] when no value is currently queued and the sender was dropped.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let head = self.inner.head.0.load(Ordering::Relaxed);
        if self.cached_tail == head {
            self.cached_tail = self.inner.tail.0.load(Ordering::Acquire);
            if self.cached_tail == head {
                if self.inner.sender_alive.load(Ordering::Acquire) {
                    return Err(TryRecvError::Empty);
                }

                self.cached_tail = self.inner.tail.0.load(Ordering::Acquire);
                if self.cached_tail == head {
                    return Err(TryRecvError::Disconnected);
                }
            }
        }

        Ok(self.read_one(head))
    }

    pub fn drain(&mut self, mut receive: impl FnMut(T)) -> usize {
        let mut drained = 0;
        loop {
            let head = self.inner.head.0.load(Ordering::Relaxed);
            if self.cached_tail == head {
                self.cached_tail = self.inner.tail.0.load(Ordering::Acquire);
                if self.cached_tail == head {
                    return drained;
                }
            }

            let value = self.read_one(head);
            drained += 1;
            receive(value);
        }
    }

    fn read_one(&self, head: u64) -> T {
        let index = ring_index(head, self.inner.mask);
        // SAFETY: tail acquire-load proved this slot is in the active [head, tail) range and was
        // initialized by the producer before publishing tail.
        let value = self.inner.slots[index]
            .value
            .with_mut(|slot| unsafe { (*slot).assume_init_read() });
        self.inner.head.0.store(head.wrapping_add(1), Ordering::Release);
        value
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.inner.receiver_alive.store(false, Ordering::Release);
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        let head = self.head.0.load(Ordering::Acquire);
        let tail = self.tail.0.load(Ordering::Acquire);
        for offset in 0..tail.wrapping_sub(head) {
            let index = ring_index(head.wrapping_add(offset), self.mask);
            // SAFETY: slots in [head, tail) are initialized and have not been consumed.
            self.slots[index].value.with_mut(|slot| unsafe {
                (*slot).assume_init_drop();
            });
        }
    }
}

#[allow(
    clippy::cast_possible_truncation,
    reason = "cursor is masked by a usize-derived ring mask before conversion"
)]
const fn ring_index(cursor: u64, mask: u64) -> usize {
    (cursor & mask) as usize
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{TryRecvError, TrySendError, bounded};

    #[test]
    fn bounded_queue_preserves_fifo_order() {
        let (mut sender, mut receiver) = bounded(2);

        sender.try_send(1).unwrap();
        sender.try_send(2).unwrap();

        assert_eq!(receiver.try_recv(), Ok(1));
        assert_eq!(receiver.try_recv(), Ok(2));
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn bounded_queue_reports_full_without_dropping_value() {
        let (mut sender, mut receiver) = bounded(1);

        sender.try_send("first").unwrap();

        assert_eq!(sender.try_send("second"), Err(TrySendError::Full("second")));
        assert_eq!(receiver.try_recv(), Ok("first"));
    }

    #[test]
    fn bounded_queue_reports_disconnected_receiver() {
        let (mut sender, receiver) = bounded(1);
        drop(receiver);

        assert_eq!(sender.try_send(7), Err(TrySendError::Disconnected(7)));
    }

    #[test]
    fn bounded_queue_reports_disconnected_sender_after_drain() {
        let (mut sender, mut receiver) = bounded(1);
        sender.try_send(7).unwrap();
        drop(sender);

        assert_eq!(receiver.try_recv(), Ok(7));
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn drain_removes_all_available_items() {
        let (mut sender, mut receiver) = bounded(3);
        sender.try_send(1).unwrap();
        sender.try_send(2).unwrap();

        let mut drained = Vec::new();
        assert_eq!(receiver.drain(|value| drained.push(value)), 2);

        assert_eq!(drained, [1, 2]);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn drops_queued_items_when_channel_is_dropped() {
        struct CountDrop(Arc<AtomicUsize>);

        impl Drop for CountDrop {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let (mut sender, receiver) = bounded(4);
        sender.try_send(CountDrop(Arc::clone(&drops))).ok();
        sender.try_send(CountDrop(Arc::clone(&drops))).ok();

        drop(receiver);
        drop(sender);

        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn supports_non_power_of_two_capacity() {
        let (mut sender, mut receiver) = bounded(3);

        sender.try_send(1).unwrap();
        sender.try_send(2).unwrap();
        sender.try_send(3).unwrap();
        assert_eq!(sender.try_send(4), Err(TrySendError::Full(4)));
        assert_eq!(receiver.try_recv(), Ok(1));
        sender.try_send(4).unwrap();

        assert_eq!(receiver.try_recv(), Ok(2));
        assert_eq!(receiver.try_recv(), Ok(3));
        assert_eq!(receiver.try_recv(), Ok(4));
    }
}
