#![allow(unsafe_code)]

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

        pub(crate) fn with<R>(&self, access: impl FnOnce(*const T) -> R) -> R {
            access(self.0.get())
        }

        pub(crate) fn with_mut<R>(&self, access: impl FnOnce(*mut T) -> R) -> R {
            access(self.0.get())
        }

        pub(crate) const fn get(&self) -> *mut T {
            self.0.get()
        }
    }
}

#[derive(Debug)]
pub struct Producer<T> {
    inner: Arc<Inner<T>>,
    cached_head: u64,
}

#[derive(Debug)]
pub struct Consumer<T> {
    inner: Arc<Inner<T>>,
    cached_tail: u64,
}

#[derive(Debug)]
pub struct BufferedProducer<T> {
    producer: Producer<T>,
    batch_size: usize,
    reserved_tail: u64,
    reserved_len: usize,
    written: usize,
}

#[derive(Debug)]
struct Inner<T> {
    slots: Box<[Slot<T>]>,
    capacity: u64,
    mask: u64,
    head: CachePadded<AtomicU64>,
    tail: CachePadded<AtomicU64>,
    producer_alive: AtomicBool,
    consumer_alive: AtomicBool,
}

#[repr(align(128))]
#[derive(Debug)]
struct CachePadded<T>(T);

#[derive(Debug)]
struct Slot<T> {
    value: UnsafeCell<T>,
}

// SAFETY: The producer only mutates slots in the reserved unpublished range and the consumer only
// reads slots in the committed range. Release/acquire publication on tail and release/acquire
// release on head prevent concurrent mutable/read access to the same slot.
unsafe impl<T: Send> Sync for Slot<T> {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryReserveError {
    Full,
    Disconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryReadError {
    Empty,
    Disconnected,
}

#[derive(Debug)]
pub struct ReservedBatch<'a, T> {
    producer: &'a mut Producer<T>,
    tail: u64,
    len: usize,
    committed: bool,
}

#[derive(Debug)]
pub struct ReadBatch<'a, T> {
    consumer: &'a mut Consumer<T>,
    head: u64,
    len: usize,
    released: bool,
}

#[must_use]
pub fn bounded<T: Default>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let capacity = capacity.max(1);
    let physical_capacity = capacity.next_power_of_two();
    let slots = std::iter::repeat_with(|| Slot {
        value: UnsafeCell::new(T::default()),
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
        producer_alive: AtomicBool::new(true),
        consumer_alive: AtomicBool::new(true),
    });
    (
        Producer {
            inner: Arc::clone(&inner),
            cached_head: 0,
        },
        Consumer { inner, cached_tail: 0 },
    )
}

#[must_use]
pub fn buffered<T: Default>(capacity: usize, batch_size: usize) -> (BufferedProducer<T>, Consumer<T>) {
    let (producer, consumer) = bounded(capacity);
    (producer.into_buffered(batch_size), consumer)
}

impl<T> Producer<T> {
    /// Reserves a contiguous unpublished batch for in-place mutation.
    ///
    /// The returned batch may be smaller than `max`, but is never empty.
    ///
    /// # Errors
    ///
    /// Returns [`TryReserveError::Full`] when no slot is available.
    /// Returns [`TryReserveError::Disconnected`] when the consumer was dropped.
    pub fn try_reserve_batch(&mut self, max: usize) -> Result<ReservedBatch<'_, T>, TryReserveError> {
        let max = max.max(1);
        let tail = self.reserve_len(1)?;
        let len = self.contiguous_available(tail).min(max);
        Ok(ReservedBatch {
            producer: self,
            tail,
            len,
            committed: false,
        })
    }

    fn reserve_len(&mut self, len: u64) -> Result<u64, TryReserveError> {
        if !self.inner.consumer_alive.load(Ordering::Acquire) {
            return Err(TryReserveError::Disconnected);
        }

        let tail = self.inner.tail.0.load(Ordering::Relaxed);
        if tail.wrapping_add(len).wrapping_sub(self.cached_head) > self.inner.capacity {
            self.cached_head = self.inner.head.0.load(Ordering::Acquire);
            if tail.wrapping_add(len).wrapping_sub(self.cached_head) > self.inner.capacity {
                return Err(TryReserveError::Full);
            }
        }
        Ok(tail)
    }

    fn contiguous_available(&self, tail: u64) -> usize {
        let used = tail.wrapping_sub(self.cached_head);
        let available = self.inner.capacity.saturating_sub(used);
        let contiguous = self.inner.slots.len() - ring_index(tail, self.inner.mask);
        usize::try_from(available).unwrap_or(usize::MAX).min(contiguous)
    }

    fn commit(&self, tail: u64, len: usize) {
        let len = u64::try_from(len).unwrap_or(u64::MAX);
        self.inner.tail.0.store(tail.wrapping_add(len), Ordering::Release);
    }

    #[must_use]
    pub fn into_buffered(self, batch_size: usize) -> BufferedProducer<T> {
        BufferedProducer {
            producer: self,
            batch_size: batch_size.max(1),
            reserved_tail: 0,
            reserved_len: 0,
            written: 0,
        }
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        self.inner.producer_alive.store(false, Ordering::Release);
    }
}

impl<T> Consumer<T> {
    /// Reads a contiguous committed batch by reference.
    ///
    /// The batch releases its slots when dropped.
    ///
    /// # Errors
    ///
    /// Returns [`TryReadError::Empty`] when no committed slot is available.
    /// Returns [`TryReadError::Disconnected`] when no committed slot is available and the producer was dropped.
    pub fn try_read_batch(&mut self, max: usize) -> Result<ReadBatch<'_, T>, TryReadError> {
        let max = max.max(1);
        let head = self.inner.head.0.load(Ordering::Relaxed);
        if self.cached_tail == head {
            self.cached_tail = self.inner.tail.0.load(Ordering::Acquire);
            if self.cached_tail == head {
                if self.inner.producer_alive.load(Ordering::Acquire) {
                    return Err(TryReadError::Empty);
                }
                self.cached_tail = self.inner.tail.0.load(Ordering::Acquire);
                if self.cached_tail == head {
                    return Err(TryReadError::Disconnected);
                }
            }
        }

        let available = self.cached_tail.wrapping_sub(head);
        let contiguous = self.inner.slots.len() - ring_index(head, self.inner.mask);
        let len = usize::try_from(available)
            .unwrap_or(usize::MAX)
            .min(contiguous)
            .min(max);
        Ok(ReadBatch {
            consumer: self,
            head,
            len,
            released: false,
        })
    }

    fn release(&self, head: u64, len: usize) {
        let len = u64::try_from(len).unwrap_or(u64::MAX);
        self.inner.head.0.store(head.wrapping_add(len), Ordering::Release);
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        self.inner.consumer_alive.store(false, Ordering::Release);
    }
}

impl<T> ReservedBatch<'_, T> {
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[cfg(not(feature = "loom"))]
    #[must_use]
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        if index >= self.len {
            return None;
        }
        Some(self.producer.slot_mut(self.tail.wrapping_add(index as u64)))
    }

    pub fn fill(&mut self, mut write: impl FnMut(usize, &mut T)) {
        for index in 0..self.len {
            self.producer
                .slot_mut_with(self.tail.wrapping_add(index as u64), |slot| write(index, slot));
        }
    }

    pub fn commit(mut self) {
        self.producer.commit(self.tail, self.len);
        self.committed = true;
    }

    pub fn commit_len(mut self, len: usize) {
        let len = len.min(self.len);
        self.producer.commit(self.tail, len);
        self.committed = true;
    }
}

impl<T> BufferedProducer<T> {
    /// Writes one item into the currently reserved batch, reserving a new batch when needed.
    ///
    /// The item becomes visible to the consumer when the batch fills or [`Self::flush`] is called.
    ///
    /// # Errors
    ///
    /// Returns [`TryReserveError::Full`] when no batch slot is available.
    /// Returns [`TryReserveError::Disconnected`] when the consumer was dropped.
    pub fn write_with(&mut self, write: impl FnOnce(&mut T)) -> Result<(), TryReserveError> {
        if self.written == self.reserved_len {
            self.flush();
            self.reserve_batch()?;
        }

        self.producer
            .slot_mut_with(self.reserved_tail.wrapping_add(self.written as u64), write);
        self.written += 1;
        if self.written == self.reserved_len {
            self.flush();
        }
        Ok(())
    }

    pub fn flush(&mut self) {
        if self.written > 0 {
            self.producer.commit(self.reserved_tail, self.written);
        }
        self.reserved_tail = 0;
        self.reserved_len = 0;
        self.written = 0;
    }

    fn reserve_batch(&mut self) -> Result<(), TryReserveError> {
        let tail = self.producer.reserve_len(1)?;
        self.reserved_tail = tail;
        self.reserved_len = self.producer.contiguous_available(tail).min(self.batch_size);
        self.written = 0;
        Ok(())
    }
}

impl<T> Drop for BufferedProducer<T> {
    fn drop(&mut self) {
        self.flush();
    }
}

impl<'ring, T> ReadBatch<'ring, T> {
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn for_each(&self, mut read: impl FnMut(usize, &T)) {
        for index in 0..self.len {
            self.consumer
                .slot_with(self.head.wrapping_add(index as u64), |slot| read(index, slot));
        }
    }

    #[cfg(not(feature = "loom"))]
    #[must_use]
    pub fn get(&self, index: usize) -> Option<&T> {
        if index >= self.len {
            return None;
        }
        Some(self.consumer.slot(self.head.wrapping_add(index as u64)))
    }

    #[cfg(not(feature = "loom"))]
    #[must_use]
    pub const fn iter<'batch>(&'batch self) -> ReadBatchIter<'batch, 'ring, T> {
        ReadBatchIter { batch: self, index: 0 }
    }

    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if !self.released {
            self.consumer.release(self.head, self.len);
            self.released = true;
        }
    }
}

impl<T> Drop for ReadBatch<'_, T> {
    fn drop(&mut self) {
        self.release_inner();
    }
}

#[cfg(not(feature = "loom"))]
#[derive(Debug)]
pub struct ReadBatchIter<'batch, 'ring, T> {
    batch: &'batch ReadBatch<'ring, T>,
    index: usize,
}

#[cfg(not(feature = "loom"))]
impl<'batch, T> Iterator for ReadBatchIter<'batch, '_, T> {
    type Item = &'batch T;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.batch.get(self.index)?;
        self.index += 1;
        Some(value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.batch.len.saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

#[cfg(not(feature = "loom"))]
impl<T> ExactSizeIterator for ReadBatchIter<'_, '_, T> {}

#[cfg(not(feature = "loom"))]
impl<'batch, 'ring, T> IntoIterator for &'batch ReadBatch<'ring, T> {
    type IntoIter = ReadBatchIter<'batch, 'ring, T>;
    type Item = &'batch T;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T> Producer<T> {
    #[allow(
        clippy::needless_pass_by_ref_mut,
        reason = "mutable slot access requires exclusive producer access even though the mutation is inside UnsafeCell"
    )]
    fn slot_mut_with<R>(&mut self, sequence: u64, access: impl FnOnce(&mut T) -> R) -> R {
        let index = ring_index(sequence, self.inner.mask);
        // SAFETY: Reserved slots are outside the committed [head, tail) range until commit.
        // Producer methods require &mut self, so the producer cannot hand out overlapping mutable
        // access through another reservation.
        self.inner.slots[index]
            .value
            .with_mut(|slot| unsafe { access(&mut *slot) })
    }

    #[cfg(not(feature = "loom"))]
    fn slot_mut(&mut self, sequence: u64) -> &mut T {
        let index = ring_index(sequence, self.inner.mask);
        // SAFETY: Reserved slots are outside the committed [head, tail) range until commit.
        // Producer methods require &mut self, so the producer cannot hand out overlapping mutable
        // access through another reservation.
        unsafe { &mut *self.inner.slots[index].value.get() }
    }
}

impl<T> Consumer<T> {
    fn slot_with<R>(&self, sequence: u64, access: impl FnOnce(&T) -> R) -> R {
        let index = ring_index(sequence, self.inner.mask);
        // SAFETY: Read batches cover only committed slots observed through an acquire-load of tail.
        // The producer cannot mutate these slots again until the batch releases them by advancing head.
        self.inner.slots[index].value.with(|slot| unsafe { access(&*slot) })
    }

    #[cfg(not(feature = "loom"))]
    fn slot(&self, sequence: u64) -> &T {
        let index = ring_index(sequence, self.inner.mask);
        // SAFETY: Read batches cover only committed slots observed through an acquire-load of tail.
        // The producer cannot mutate these slots again until the batch releases them by advancing head.
        unsafe { &*self.inner.slots[index].value.get() }
    }
}

fn ring_index(sequence: u64, mask: u64) -> usize {
    usize::try_from(sequence & mask).unwrap_or_default()
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::{TryReadError, TryReserveError, bounded, buffered};

    #[test]
    fn reserves_writes_commits_and_reads_slot() {
        let (mut producer, mut consumer) = bounded::<u64>(4);

        write_one(&mut producer, 42);

        let batch = consumer.try_read_batch(8).unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.get(0), Some(&42));
        batch.release();
        assert!(matches!(consumer.try_read_batch(1), Err(TryReadError::Empty)));
    }

    #[test]
    fn dropped_reservation_is_not_published() {
        let (mut producer, mut consumer) = bounded::<u64>(2);

        {
            let mut batch = producer.try_reserve_batch(1).unwrap();
            *batch.get_mut(0).unwrap() = 7;
        }

        assert!(matches!(consumer.try_read_batch(1), Err(TryReadError::Empty)));

        write_one(&mut producer, 9);

        let batch = consumer.try_read_batch(1).unwrap();
        assert_eq!(batch.get(0), Some(&9));
    }

    #[test]
    fn full_until_consumer_releases_batch() {
        let (mut producer, mut consumer) = bounded::<u64>(2);

        for value in [1, 2] {
            write_one(&mut producer, value);
        }
        assert!(matches!(producer.try_reserve_batch(1), Err(TryReserveError::Full)));

        let batch = consumer.try_read_batch(2).unwrap();
        assert_eq!(batch.iter().copied().collect::<Vec<_>>(), vec![1, 2]);
        assert!(matches!(producer.try_reserve_batch(1), Err(TryReserveError::Full)));
        drop(batch);

        write_one(&mut producer, 3);
        assert_eq!(
            consumer.try_read_batch(2).unwrap().iter().copied().collect::<Vec<_>>(),
            vec![3]
        );
    }

    #[test]
    fn reserve_and_read_batches_are_contiguous_and_ordered() {
        let (mut producer, mut consumer) = bounded::<u64>(4);

        let mut first = producer.try_reserve_batch(4).unwrap();
        assert_eq!(first.len(), 4);
        first.fill(|index, value| *value = index as u64);
        first.commit();

        let batch = consumer.try_read_batch(3).unwrap();
        assert_eq!(batch.iter().copied().collect::<Vec<_>>(), vec![0, 1, 2]);
        drop(batch);

        let mut second = producer.try_reserve_batch(3).unwrap();
        assert_eq!(second.len(), 3);
        second.fill(|index, value| *value = 10 + index as u64);
        second.commit();

        let tail = consumer.try_read_batch(8).unwrap();
        assert_eq!(tail.iter().copied().collect::<Vec<_>>(), vec![3]);
        drop(tail);

        let wrapped = consumer.try_read_batch(8).unwrap();
        assert_eq!(wrapped.iter().copied().collect::<Vec<_>>(), vec![10, 11, 12]);
    }

    #[test]
    fn disconnects_are_reported_after_buffer_is_drained() {
        let (mut producer, mut consumer) = bounded::<u64>(2);
        write_one(&mut producer, 1);
        drop(producer);

        assert_eq!(consumer.try_read_batch(2).unwrap().get(0), Some(&1));
        assert!(matches!(consumer.try_read_batch(1), Err(TryReadError::Disconnected)));

        let (mut producer, consumer) = bounded::<u64>(1);
        drop(consumer);
        assert!(matches!(
            producer.try_reserve_batch(1),
            Err(TryReserveError::Disconnected)
        ));
    }

    #[test]
    fn overwriting_released_slot_drops_previous_value_once() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Clone, Default)]
        struct CountDrop(Option<Arc<AtomicUsize>>);

        impl Drop for CountDrop {
            fn drop(&mut self) {
                if let Some(drops) = &self.0 {
                    drops.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let (mut producer, mut consumer) = bounded::<CountDrop>(1);

        write_drop_value(&mut producer, CountDrop(Some(Arc::clone(&drops))));
        drop(consumer.try_read_batch(1).unwrap());
        assert_eq!(drops.load(Ordering::Relaxed), 0);

        write_drop_value(&mut producer, CountDrop(Some(Arc::clone(&drops))));
        assert_eq!(drops.load(Ordering::Relaxed), 1);

        drop(producer);
        drop(consumer);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn batch_drop_releases_capacity_without_explicit_release() {
        let (mut producer, mut consumer) = bounded::<u64>(2);
        let mut batch = producer.try_reserve_batch(2).unwrap();
        batch.fill(|index, value| *value = index as u64 + 1);
        batch.commit();

        let batch = consumer.try_read_batch(2).unwrap();
        assert_eq!(batch.iter().copied().collect::<Vec<_>>(), vec![1, 2]);
        drop(batch);

        let mut next = producer.try_reserve_batch(2).unwrap();
        assert_eq!(next.len(), 2);
        next.fill(|index, value| *value = index as u64 + 3);
        next.commit();

        assert_eq!(
            consumer.try_read_batch(2).unwrap().iter().copied().collect::<Vec<_>>(),
            vec![3, 4]
        );
    }

    #[test]
    fn partial_batch_commit_publishes_only_requested_prefix() {
        let (mut producer, mut consumer) = bounded::<u64>(4);
        let mut batch = producer.try_reserve_batch(4).unwrap();
        batch.fill(|index, value| *value = index as u64 + 1);
        batch.commit_len(2);

        let batch = consumer.try_read_batch(4).unwrap();
        assert_eq!(batch.iter().copied().collect::<Vec<_>>(), vec![1, 2]);
        drop(batch);

        let mut next = producer.try_reserve_batch(4).unwrap();
        next.fill(|index, value| *value = index as u64 + 10);
        next.commit();

        assert_eq!(
            consumer.try_read_batch(4).unwrap().iter().copied().collect::<Vec<_>>(),
            vec![10, 11]
        );
    }

    #[test]
    fn buffered_producer_commits_on_flush_or_full_batch() {
        let (mut producer, mut consumer) = buffered::<u64>(4, 2);
        producer.write_with(|value| *value = 1).unwrap();
        assert!(matches!(consumer.try_read_batch(1), Err(TryReadError::Empty)));

        producer.flush();
        assert_eq!(consumer.try_read_batch(1).unwrap().get(0), Some(&1));

        producer.write_with(|value| *value = 2).unwrap();
        assert!(matches!(consumer.try_read_batch(1), Err(TryReadError::Empty)));
        producer.write_with(|value| *value = 3).unwrap();

        assert_eq!(
            consumer.try_read_batch(2).unwrap().iter().copied().collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    fn write_one(producer: &mut super::Producer<u64>, value: u64) {
        let result = producer.try_reserve_batch(1);
        assert!(result.is_ok());
        let Ok(mut batch) = result else {
            return;
        };
        assert!(!batch.is_empty(), "reserved test batch should contain one slot");
        batch.fill(|index, slot| {
            if index == 0 {
                *slot = value;
            }
        });
        batch.commit_len(1);
    }

    fn write_drop_value<T: Default>(producer: &mut super::Producer<T>, value: T) {
        let result = producer.try_reserve_batch(1);
        assert!(result.is_ok());
        let Ok(mut batch) = result else {
            return;
        };
        assert!(!batch.is_empty(), "reserved test batch should contain one slot");
        let mut value = Some(value);
        batch.fill(|index, slot| {
            if index == 0
                && let Some(next) = value.take()
            {
                *slot = next;
            }
        });
        batch.commit_len(1);
    }
}
