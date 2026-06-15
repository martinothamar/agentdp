use std::cell::RefCell;
use std::rc::Rc;

use crate::network::NetworkLimits;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{kind:?} buffer pool exhausted for requested capacity {capacity}")]
pub(crate) struct PoolExhausted {
    kind: PoolKind,
    capacity: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PoolKind {
    Frame,
    Byte,
}

#[derive(Debug, Clone)]
pub(crate) struct BufferPool {
    frames: FramePool,
    bytes: BytePool,
    limits: Rc<NetworkLimits>,
}

#[cfg(any(test, feature = "simulation"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BufferPoolSnapshot {
    pub(crate) frame_checked_out: usize,
    pub(crate) byte_checked_out: usize,
    pub(crate) small_byte_available: usize,
    pub(crate) medium_byte_available: usize,
    pub(crate) tcp_byte_available: usize,
}

impl BufferPool {
    pub(crate) fn new(limits: NetworkLimits) -> Self {
        let limits = Rc::new(limits);
        Self {
            frames: FramePool::new(limits.clone()),
            bytes: BytePool::new(limits.clone()),
            limits,
        }
    }

    pub(crate) fn try_frame(&self) -> Result<FrameBuf, PoolExhausted> {
        self.frames.try_take(self.limits.frame_buffer_capacity)
    }

    pub(crate) fn try_frame_with_capacity(&self, capacity: usize) -> Result<FrameBuf, PoolExhausted> {
        self.frames.try_take(capacity)
    }

    pub(crate) fn try_byte_with_capacity(&self, capacity: usize) -> Result<ByteBuf, PoolExhausted> {
        self.bytes.try_take(capacity)
    }

    pub(crate) fn try_tcp_byte(&self) -> Result<ByteBuf, PoolExhausted> {
        self.bytes.try_take(self.limits.tcp_byte_capacity)
    }

    pub(crate) fn tcp_byte_capacity(&self) -> usize {
        self.limits.tcp_byte_capacity
    }

    pub(crate) fn limits(&self) -> &NetworkLimits {
        &self.limits
    }

    pub(crate) fn prewarm_instance_network(&self) {
        self.frames.prewarm(
            self.limits.frame_buffer_pool_capacity,
            self.limits.max_pooled_frame_capacity,
        );
        self.bytes
            .prewarm(ByteClass::Small, self.limits.small_byte_pool_capacity);
        self.bytes
            .prewarm(ByteClass::Medium, self.limits.medium_byte_pool_capacity);
        self.bytes.prewarm(ByteClass::Tcp, self.limits.tcp_byte_pool_capacity);
    }

    #[cfg(test)]
    pub(crate) fn assert_drained(&self) {
        self.frames.assert_drained();
        self.bytes.assert_drained();
    }

    #[cfg(any(test, feature = "simulation"))]
    pub(crate) fn snapshot(&self) -> BufferPoolSnapshot {
        let bytes = self.bytes.inner.borrow();
        BufferPoolSnapshot {
            frame_checked_out: self.frames.checked_out.get(),
            byte_checked_out: self.bytes.checked_out.get(),
            small_byte_available: bytes.small.len(),
            medium_byte_available: bytes.medium.len(),
            tcp_byte_available: bytes.tcp.len(),
        }
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new(NetworkLimits::default())
    }
}

#[derive(Debug, Clone)]
struct FramePool {
    inner: Rc<RefCell<Vec<Vec<u8>>>>,
    limits: Rc<NetworkLimits>,
    #[cfg(any(test, feature = "simulation"))]
    checked_out: Rc<std::cell::Cell<usize>>,
}

#[derive(Debug, Clone)]
struct BytePool {
    inner: Rc<RefCell<ByteBuckets>>,
    limits: Rc<NetworkLimits>,
    #[cfg(any(test, feature = "simulation"))]
    checked_out: Rc<std::cell::Cell<usize>>,
}

impl FramePool {
    fn new(limits: Rc<NetworkLimits>) -> Self {
        Self {
            inner: Rc::default(),
            limits,
            #[cfg(any(test, feature = "simulation"))]
            checked_out: Rc::default(),
        }
    }

    fn try_take(&self, capacity: usize) -> Result<FrameBuf, PoolExhausted> {
        #[cfg(any(test, feature = "simulation"))]
        let checked_out = self.checked_out.get();
        let capacity = capacity.max(self.limits.frame_buffer_capacity);
        if capacity > self.limits.max_pooled_frame_capacity {
            return Err(PoolExhausted {
                kind: PoolKind::Frame,
                capacity,
            });
        }
        let Some(bytes) = self.inner.borrow_mut().pop() else {
            return Err(PoolExhausted {
                kind: PoolKind::Frame,
                capacity,
            });
        };
        if bytes.capacity() < capacity {
            self.inner.borrow_mut().push(bytes);
            return Err(PoolExhausted {
                kind: PoolKind::Frame,
                capacity,
            });
        }
        #[cfg(any(test, feature = "simulation"))]
        self.checked_out.set(checked_out.saturating_add(1));
        Ok(FrameBuf {
            bytes,
            pool: self.clone(),
        })
    }

    fn recycle(&self, mut bytes: Vec<u8>) {
        #[cfg(any(test, feature = "simulation"))]
        decrement(&self.checked_out, "frame");
        if bytes.capacity() > self.limits.max_pooled_frame_capacity {
            return;
        }
        bytes.clear();
        let mut frames = self.inner.borrow_mut();
        if frames.len() < self.limits.frame_buffer_pool_capacity {
            frames.push(bytes);
        }
    }

    fn prewarm(&self, count: usize, capacity: usize) {
        let mut frames = self.inner.borrow_mut();
        while frames.len() < count {
            frames.push(Vec::with_capacity(capacity));
        }
    }

    #[cfg(test)]
    fn assert_drained(&self) {
        assert_eq!(self.checked_out.get(), 0, "frame buffers still checked out");
    }
}

impl BytePool {
    fn new(limits: Rc<NetworkLimits>) -> Self {
        Self {
            inner: Rc::default(),
            limits,
            #[cfg(any(test, feature = "simulation"))]
            checked_out: Rc::default(),
        }
    }

    fn try_take(&self, capacity: usize) -> Result<ByteBuf, PoolExhausted> {
        #[cfg(any(test, feature = "simulation"))]
        let checked_out = self.checked_out.get();
        let class = ByteClass::for_capacity(capacity, &self.limits);
        let Some(bytes) = self.inner.borrow_mut().take(class) else {
            return Err(PoolExhausted {
                kind: PoolKind::Byte,
                capacity,
            });
        };
        if bytes.capacity() < capacity {
            self.inner.borrow_mut().recycle(class, bytes, &self.limits);
            return Err(PoolExhausted {
                kind: PoolKind::Byte,
                capacity,
            });
        }
        #[cfg(any(test, feature = "simulation"))]
        self.checked_out.set(checked_out.saturating_add(1));
        Ok(ByteBuf {
            bytes,
            pool: self.clone(),
            class,
        })
    }

    fn recycle(&self, mut bytes: Vec<u8>, class: ByteClass) {
        #[cfg(any(test, feature = "simulation"))]
        decrement(&self.checked_out, "byte buffer");
        if class.can_pool(bytes.capacity(), &self.limits) {
            bytes.clear();
        } else {
            bytes = Vec::with_capacity(class.capacity(&self.limits));
        }
        self.inner.borrow_mut().recycle(class, bytes, &self.limits);
    }

    fn prewarm(&self, class: ByteClass, count: usize) {
        self.inner.borrow_mut().prewarm(class, count, &self.limits);
    }

    #[cfg(test)]
    fn assert_drained(&self) {
        assert_eq!(self.checked_out.get(), 0, "byte buffers still checked out");
    }
}

#[derive(Debug, Default)]
struct ByteBuckets {
    small: Vec<Vec<u8>>,
    medium: Vec<Vec<u8>>,
    tcp: Vec<Vec<u8>>,
}

impl ByteBuckets {
    fn take(&mut self, class: ByteClass) -> Option<Vec<u8>> {
        if class == ByteClass::Oversized {
            return None;
        }
        class.bucket_mut(self).pop()
    }

    fn recycle(&mut self, class: ByteClass, bytes: Vec<u8>, limits: &NetworkLimits) {
        let bucket = class.bucket_mut(self);
        if bucket.len() < class.max_pooled(limits) {
            bucket.push(bytes);
        }
    }

    fn prewarm(&mut self, class: ByteClass, count: usize, limits: &NetworkLimits) {
        let bucket = class.bucket_mut(self);
        while bucket.len() < count {
            bucket.push(Vec::with_capacity(class.capacity(limits)));
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ByteClass {
    Small,
    Medium,
    Tcp,
    Oversized,
}

impl ByteClass {
    const fn for_capacity(capacity: usize, limits: &NetworkLimits) -> Self {
        if capacity <= limits.small_byte_capacity {
            Self::Small
        } else if capacity <= limits.medium_byte_capacity {
            Self::Medium
        } else if capacity <= limits.tcp_byte_capacity {
            Self::Tcp
        } else {
            Self::Oversized
        }
    }

    const fn capacity(self, limits: &NetworkLimits) -> usize {
        match self {
            Self::Small => limits.small_byte_capacity,
            Self::Medium => limits.medium_byte_capacity,
            Self::Tcp => limits.tcp_byte_capacity,
            Self::Oversized => 0,
        }
    }

    const fn max_pooled(self, limits: &NetworkLimits) -> usize {
        match self {
            Self::Small => limits.small_byte_pool_capacity,
            Self::Medium => limits.medium_byte_pool_capacity,
            Self::Tcp => limits.tcp_byte_pool_capacity,
            Self::Oversized => 0,
        }
    }

    const fn can_pool(self, capacity: usize, limits: &NetworkLimits) -> bool {
        match self {
            Self::Small => capacity <= limits.small_byte_capacity,
            Self::Medium => capacity <= limits.medium_byte_capacity,
            Self::Tcp => capacity <= limits.tcp_byte_capacity,
            Self::Oversized => false,
        }
    }

    fn bucket_mut(self, buckets: &mut ByteBuckets) -> &mut Vec<Vec<u8>> {
        match self {
            Self::Small => &mut buckets.small,
            Self::Medium => &mut buckets.medium,
            Self::Tcp => &mut buckets.tcp,
            Self::Oversized => unreachable!("oversized buffers are not pooled"),
        }
    }
}

#[cfg(any(test, feature = "simulation"))]
fn decrement(counter: &std::cell::Cell<usize>, label: &str) {
    let current = counter.get();
    assert!(current > 0, "recycled {label} that was not checked out");
    counter.set(current - 1);
}

#[derive(Debug)]
pub struct FrameBuf {
    bytes: Vec<u8>,
    pool: FramePool,
}

impl FrameBuf {
    #[must_use]
    pub const fn len(&self) -> usize {
        self.bytes.len()
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub const fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        &mut self.bytes
    }

    pub(crate) fn resize_zeroed(&mut self, len: usize) {
        self.bytes.resize(len, 0);
    }
}

impl AsRef<[u8]> for FrameBuf {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Drop for FrameBuf {
    fn drop(&mut self) {
        self.pool.recycle(std::mem::take(&mut self.bytes));
    }
}

#[derive(Debug)]
pub(crate) struct ByteBuf {
    bytes: Vec<u8>,
    pool: BytePool,
    class: ByteClass,
}

impl ByteBuf {
    #[must_use]
    pub(crate) const fn len(&self) -> usize {
        self.bytes.len()
    }

    #[must_use]
    pub(crate) const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    #[must_use]
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) const fn as_mut_vec(&mut self) -> &mut Vec<u8> {
        &mut self.bytes
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.bytes
    }

    pub(crate) fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    pub(crate) fn resize_zeroed(&mut self, len: usize) {
        self.bytes.resize(len, 0);
    }

    pub(crate) fn truncate(&mut self, len: usize) {
        self.bytes.truncate(len);
    }
}

impl AsRef<[u8]> for ByteBuf {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Drop for ByteBuf {
    fn drop(&mut self) {
        self.pool.recycle(std::mem::take(&mut self.bytes), self.class);
    }
}

#[cfg(test)]
mod tests {
    use crate::network::NetworkLimits;

    use super::BufferPool;

    fn prewarmed_pool() -> BufferPool {
        let pool = BufferPool::default();
        pool.prewarm_instance_network();
        pool
    }

    #[test]
    fn cloned_pool_shares_checkout_accounting() {
        let pool = prewarmed_pool();
        let clone = pool.clone();

        let frame = pool.try_frame().expect("prewarmed frame");
        let io = clone.try_byte_with_capacity(64).expect("prewarmed byte buffer");

        drop(frame);
        drop(io);
        pool.assert_drained();
        clone.assert_drained();
    }

    #[test]
    fn recycled_frame_buffers_are_cleared_and_reused() {
        let pool = prewarmed_pool();
        let mut frame = pool.try_frame_with_capacity(4096).expect("prewarmed frame");
        frame.resize_zeroed(1024);
        assert_eq!(frame.len(), 1024);

        drop(frame);
        let warmed = pool.frames.inner.borrow().len();

        let frame = pool.try_frame().expect("recycled frame");
        assert!(frame.is_empty());
        assert!(frame.bytes.capacity() >= 4096);
        assert_eq!(pool.frames.inner.borrow().len(), warmed - 1);

        drop(frame);
        pool.assert_drained();
    }

    #[test]
    fn recycled_byte_buffers_are_cleared_and_reused() {
        let pool = prewarmed_pool();
        let mut buffer = pool.try_byte_with_capacity(4096).expect("prewarmed byte buffer");
        buffer.extend_from_slice(&[7; 1024]);
        assert_eq!(buffer.len(), 1024);

        drop(buffer);
        let warmed_medium = pool.bytes.inner.borrow().medium.len();

        let buffer = pool.try_byte_with_capacity(4096).expect("recycled byte buffer");
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.bytes.capacity(), pool.limits.medium_byte_capacity);
        assert_eq!(pool.bytes.inner.borrow().medium.len(), warmed_medium - 1);

        drop(buffer);
        pool.assert_drained();
    }

    #[test]
    fn grown_byte_buffers_replenish_their_original_pool_class() {
        let pool = prewarmed_pool();
        let mut buffer = pool.try_byte_with_capacity(4096).expect("prewarmed medium buffer");
        buffer.as_mut_vec().reserve_exact(pool.limits.medium_byte_capacity + 1);

        drop(buffer);

        assert_eq!(
            pool.bytes.inner.borrow().medium.len(),
            pool.limits.medium_byte_pool_capacity
        );
        let buffer = pool.try_byte_with_capacity(4096).expect("replenished medium buffer");
        assert_eq!(buffer.bytes.capacity(), pool.limits.medium_byte_capacity);
        drop(buffer);
        pool.assert_drained();
    }

    #[test]
    fn byte_pool_reuses_matching_capacity_class() {
        let pool = prewarmed_pool();
        let small = pool.try_byte_with_capacity(64).expect("prewarmed small buffer");
        let medium = pool
            .try_byte_with_capacity(pool.limits.small_byte_capacity + 1)
            .expect("prewarmed medium buffer");
        let tcp = pool.try_tcp_byte().expect("prewarmed tcp buffer");

        assert_eq!(small.bytes.capacity(), pool.limits.small_byte_capacity);
        assert_eq!(medium.bytes.capacity(), pool.limits.medium_byte_capacity);
        assert_eq!(tcp.bytes.capacity(), pool.limits.tcp_byte_capacity);

        drop(small);
        drop(medium);
        drop(tcp);

        assert_eq!(
            pool.bytes.inner.borrow().small.len(),
            pool.limits.small_byte_pool_capacity
        );
        assert_eq!(
            pool.bytes.inner.borrow().medium.len(),
            pool.limits.medium_byte_pool_capacity
        );
        assert_eq!(pool.bytes.inner.borrow().tcp.len(), pool.limits.tcp_byte_pool_capacity);
        pool.assert_drained();
    }

    #[test]
    fn oversized_buffers_are_rejected() {
        let pool = prewarmed_pool();

        assert!(
            pool.try_frame_with_capacity(pool.limits.max_pooled_frame_capacity + 1)
                .is_err()
        );
        assert!(pool.try_byte_with_capacity(pool.limits.tcp_byte_capacity + 1).is_err());
        pool.assert_drained();
    }

    #[test]
    fn pool_retains_at_most_configured_capacity() {
        let limits = NetworkLimits {
            frame_buffer_pool_capacity: 2,
            small_byte_pool_capacity: 2,
            ..NetworkLimits::default()
        };
        let pool = BufferPool::new(limits);
        pool.prewarm_instance_network();
        let frames: Vec<_> = (0..pool.limits.frame_buffer_pool_capacity)
            .map(|_| pool.try_frame().expect("prewarmed frame"))
            .collect();
        let io_buffers: Vec<_> = (0..pool.limits.small_byte_pool_capacity)
            .map(|_| pool.try_byte_with_capacity(8).expect("prewarmed small buffer"))
            .collect();

        for frame in frames {
            drop(frame);
        }
        for buffer in io_buffers {
            drop(buffer);
        }

        assert_eq!(pool.frames.inner.borrow().len(), pool.limits.frame_buffer_pool_capacity);
        assert_eq!(
            pool.bytes.inner.borrow().small.len(),
            pool.limits.small_byte_pool_capacity
        );
        pool.assert_drained();
    }

    #[test]
    fn warmed_pool_take_recycle_hot_path_reuses_buffers() {
        let pool = prewarmed_pool();

        for _ in 0..1024 {
            let mut frame = pool.try_frame_with_capacity(1500).expect("prewarmed frame");
            frame.resize_zeroed(1500);
            drop(frame);

            let mut io = pool.try_tcp_byte().expect("prewarmed tcp buffer");
            io.extend_from_slice(&[1; 64]);
            drop(io);
        }

        assert_eq!(pool.frames.inner.borrow().len(), pool.limits.frame_buffer_pool_capacity);
        assert_eq!(pool.bytes.inner.borrow().tcp.len(), pool.limits.tcp_byte_pool_capacity);
        pool.assert_drained();
    }

    #[test]
    #[should_panic(expected = "frame buffers still checked out")]
    fn assert_drained_detects_unrecycled_frame() {
        let pool = prewarmed_pool();
        let _frame = pool.try_frame().expect("prewarmed frame");

        pool.assert_drained();
    }

    #[test]
    #[should_panic(expected = "byte buffers still checked out")]
    fn assert_drained_detects_unrecycled_byte_buffer() {
        let pool = prewarmed_pool();
        let _io = pool.try_byte_with_capacity(64).expect("prewarmed byte buffer");

        pool.assert_drained();
    }

    #[test]
    fn cold_pool_rejects_runtime_checkout() {
        let pool = BufferPool::default();

        assert!(pool.try_frame().is_err());
        assert!(pool.try_byte_with_capacity(64).is_err());
    }
}
