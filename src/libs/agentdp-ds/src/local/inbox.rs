use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

#[derive(Debug)]
pub struct Sender<T> {
    inner: Rc<RefCell<Inner<T>>>,
}

#[derive(Debug)]
pub struct Receiver<T> {
    inner: Option<Rc<RefCell<Inner<T>>>>,
}

#[derive(Debug)]
pub struct Send<'a, T> {
    sender: &'a Sender<T>,
    value: Option<T>,
}

#[derive(Debug)]
pub struct Recv<'a, T> {
    receiver: &'a mut Receiver<T>,
}

#[derive(Debug)]
struct Inner<T> {
    queue: VecDeque<T>,
    capacity: usize,
    sender_count: usize,
    receiver_alive: bool,
    sender_wakers: Vec<Waker>,
    receiver_waker: Option<Waker>,
}

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
    let inner = Rc::new(RefCell::new(Inner {
        queue: VecDeque::with_capacity(capacity.max(1)),
        capacity: capacity.max(1),
        sender_count: 1,
        receiver_alive: true,
        sender_wakers: Vec::new(),
        receiver_waker: None,
    }));
    (
        Sender {
            inner: Rc::clone(&inner),
        },
        Receiver { inner: Some(inner) },
    )
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.inner.borrow_mut().sender_count += 1;
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl<T> Sender<T> {
    /// Attempts to enqueue a value without waiting for capacity.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] with the original value when the bounded inbox is full.
    /// Returns [`TrySendError::Disconnected`] with the original value when the receiver was dropped.
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let mut inner = self.inner.borrow_mut();
        if !inner.receiver_alive {
            return Err(TrySendError::Disconnected(value));
        }
        if inner.queue.len() == inner.capacity {
            return Err(TrySendError::Full(value));
        }
        inner.queue.push_back(value);
        let receiver = inner.take_receiver_waker();
        drop(inner);
        wake(receiver);
        Ok(())
    }

    pub const fn send(&self, value: T) -> Send<'_, T> {
        Send {
            sender: self,
            value: Some(value),
        }
    }

    fn register_waker(&self, waker: &Waker) {
        let mut inner = self.inner.borrow_mut();
        if inner.sender_wakers.iter().any(|registered| registered.will_wake(waker)) {
            return;
        }
        inner.sender_wakers.push(waker.clone());
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut inner = self.inner.borrow_mut();
        inner.sender_count = inner.sender_count.saturating_sub(1);
        if inner.sender_count == 0 {
            let receiver = inner.take_receiver_waker();
            drop(inner);
            wake(receiver);
        }
    }
}

impl<T> Future for Send<'_, T> {
    type Output = Result<(), TrySendError<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some(value) = this.value.take() else {
            return Poll::Ready(Ok(()));
        };

        match this.sender.try_send(value) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(TrySendError::Disconnected(value)) => Poll::Ready(Err(TrySendError::Disconnected(value))),
            Err(TrySendError::Full(value)) => {
                this.sender.register_waker(cx.waker());
                match this.sender.try_send(value) {
                    Ok(()) => Poll::Ready(Ok(())),
                    Err(TrySendError::Disconnected(value)) => Poll::Ready(Err(TrySendError::Disconnected(value))),
                    Err(TrySendError::Full(value)) => {
                        this.value = Some(value);
                        Poll::Pending
                    }
                }
            }
        }
    }
}

impl<T> Unpin for Send<'_, T> {}

impl<T> Receiver<T> {
    /// Attempts to dequeue a value without waiting for one to arrive.
    ///
    /// # Errors
    ///
    /// Returns [`TryRecvError::Empty`] when no value is currently queued and at least one sender is alive.
    /// Returns [`TryRecvError::Disconnected`] when no value is currently queued and all senders were dropped.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let Some(inner) = &self.inner else {
            return Err(TryRecvError::Disconnected);
        };
        let mut inner = inner.borrow_mut();
        if let Some(value) = inner.queue.pop_front() {
            let senders = inner.take_sender_wakers();
            drop(inner);
            wake_all(senders);
            return Ok(value);
        }
        if inner.sender_count == 0 {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    pub const fn recv(&mut self) -> Recv<'_, T> {
        Recv { receiver: self }
    }

    pub fn drain(&mut self, mut receive: impl FnMut(T)) -> usize {
        let Some(inner) = &self.inner else {
            return 0;
        };
        let mut inner = inner.borrow_mut();
        let drained = inner.queue.len();
        while let Some(value) = inner.queue.pop_front() {
            receive(value);
        }
        if drained > 0 {
            let senders = inner.take_sender_wakers();
            drop(inner);
            wake_all(senders);
        }
        drained
    }

    fn register_waker(&self, waker: &Waker) {
        if let Some(inner) = &self.inner {
            inner.borrow_mut().receiver_waker = Some(waker.clone());
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let mut inner = inner.borrow_mut();
        inner.receiver_alive = false;
        inner.queue.clear();
        let senders = inner.take_sender_wakers();
        drop(inner);
        wake_all(senders);
    }
}

impl<T> Future for Recv<'_, T> {
    type Output = Result<T, TryRecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this.receiver.try_recv() {
            Ok(value) => Poll::Ready(Ok(value)),
            Err(TryRecvError::Disconnected) => Poll::Ready(Err(TryRecvError::Disconnected)),
            Err(TryRecvError::Empty) => {
                this.receiver.register_waker(cx.waker());
                match this.receiver.try_recv() {
                    Ok(value) => Poll::Ready(Ok(value)),
                    Err(TryRecvError::Disconnected) => Poll::Ready(Err(TryRecvError::Disconnected)),
                    Err(TryRecvError::Empty) => Poll::Pending,
                }
            }
        }
    }
}

impl<T> Unpin for Recv<'_, T> {}

impl<T> Inner<T> {
    fn take_sender_wakers(&mut self) -> Vec<Waker> {
        std::mem::take(&mut self.sender_wakers)
    }

    const fn take_receiver_waker(&mut self) -> Option<Waker> {
        self.receiver_waker.take()
    }
}

fn wake(waker: Option<Waker>) {
    if let Some(waker) = waker {
        waker.wake();
    }
}

fn wake_all(wakers: Vec<Waker>) {
    for waker in wakers {
        waker.wake();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{TryRecvError, TrySendError, bounded};

    #[tokio::test(flavor = "local")]
    async fn cloned_senders_feed_one_receiver() {
        let (sender, mut receiver) = bounded(2);
        let other = sender.clone();

        sender.send(1).await.unwrap();
        other.send(2).await.unwrap();

        assert_eq!(receiver.recv().await, Ok(1));
        assert_eq!(receiver.recv().await, Ok(2));
    }

    #[tokio::test(flavor = "local")]
    async fn send_waits_for_receiver_capacity() {
        let (sender, mut receiver) = bounded(1);
        let other = sender.clone();

        sender.try_send(1).unwrap();

        let producer = async {
            other.send(2).await.unwrap();
        };
        let consumer = async {
            assert_eq!(receiver.recv().await, Ok(1));
            assert_eq!(receiver.recv().await, Ok(2));
        };

        tokio::join!(producer, consumer);
    }

    #[tokio::test(flavor = "local")]
    async fn recv_waits_for_sender_value() {
        let (sender, mut receiver) = bounded(1);

        let producer = async {
            sender.send(7).await.unwrap();
        };
        let consumer = async {
            assert_eq!(receiver.recv().await, Ok(7));
        };

        tokio::join!(producer, consumer);
    }

    #[tokio::test(flavor = "local")]
    async fn receiver_disconnect_rejects_send() {
        let (sender, receiver) = bounded(1);
        drop(receiver);

        assert_eq!(sender.send(9).await, Err(TrySendError::Disconnected(9)));
    }

    #[tokio::test(flavor = "local")]
    async fn receiver_disconnect_wakes_all_waiting_senders() {
        let (sender, receiver) = bounded(1);
        let other = sender.clone();
        sender.try_send(1).unwrap();

        let first = async {
            assert_eq!(sender.send(2).await, Err(TrySendError::Disconnected(2)));
        };
        let second = async {
            assert_eq!(other.send(3).await, Err(TrySendError::Disconnected(3)));
        };
        let consumer = async {
            drop(receiver);
        };

        tokio::join!(first, second, consumer);
    }

    #[tokio::test(flavor = "local")]
    async fn receiver_observes_disconnect_after_all_senders_drop() {
        let (sender, mut receiver) = bounded::<u32>(1);
        let other = sender.clone();
        drop(sender);

        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
        drop(other);

        assert_eq!(receiver.recv().await, Err(TryRecvError::Disconnected));
    }

    #[test]
    fn try_send_reports_full_without_dropping_value() {
        let (sender, mut receiver) = bounded(1);

        sender.try_send("first").unwrap();

        assert_eq!(sender.try_send("second"), Err(TrySendError::Full("second")));
        assert_eq!(receiver.try_recv(), Ok("first"));
    }

    #[test]
    fn drain_removes_all_available_items() {
        let (sender, mut receiver) = bounded(3);
        sender.try_send(1).unwrap();
        sender.try_send(2).unwrap();

        let mut drained = Vec::new();
        assert_eq!(receiver.drain(|value| drained.push(value)), 2);

        assert_eq!(drained, [1, 2]);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn drops_queued_items_when_receiver_is_dropped() {
        struct CountDrop(Arc<AtomicUsize>);

        impl Drop for CountDrop {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let (sender, receiver) = bounded(4);
        sender.try_send(CountDrop(Arc::clone(&drops))).ok();
        sender.try_send(CountDrop(Arc::clone(&drops))).ok();

        drop(receiver);
        drop(sender);

        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }
}
