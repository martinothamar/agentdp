use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

#[derive(Debug)]
pub struct Sender<T> {
    inner: Option<Rc<RefCell<Inner<T>>>>,
}

#[derive(Debug)]
pub struct Receiver<T> {
    inner: Option<Rc<RefCell<Inner<T>>>>,
}

#[derive(Debug)]
pub struct Send<'a, T> {
    sender: &'a mut Sender<T>,
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
    sender_alive: bool,
    receiver_alive: bool,
    sender_waker: Option<Waker>,
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
        sender_alive: true,
        receiver_alive: true,
        sender_waker: None,
        receiver_waker: None,
    }));
    (
        Sender {
            inner: Some(Rc::clone(&inner)),
        },
        Receiver { inner: Some(inner) },
    )
}

impl<T> Sender<T> {
    /// Attempts to enqueue a value without waiting for capacity.
    ///
    /// # Errors
    ///
    /// Returns [`TrySendError::Full`] with the original value when the bounded queue is full.
    /// Returns [`TrySendError::Disconnected`] with the original value when the receiver was dropped.
    pub fn try_send(&mut self, value: T) -> Result<(), TrySendError<T>> {
        let Some(inner) = &self.inner else {
            return Err(TrySendError::Disconnected(value));
        };
        let mut inner = inner.borrow_mut();
        if !inner.receiver_alive {
            return Err(TrySendError::Disconnected(value));
        }
        if inner.queue.len() == inner.capacity {
            return Err(TrySendError::Full(value));
        }
        inner.queue.push_back(value);
        inner.wake_receiver();
        Ok(())
    }

    pub const fn send(&mut self, value: T) -> Send<'_, T> {
        Send {
            sender: self,
            value: Some(value),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let mut inner = inner.borrow_mut();
        inner.sender_alive = false;
        inner.wake_receiver();
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

impl<T> Sender<T> {
    fn register_waker(&self, waker: &Waker) {
        if let Some(inner) = &self.inner {
            inner.borrow_mut().sender_waker = Some(waker.clone());
        }
    }
}

impl<T> Receiver<T> {
    /// Attempts to dequeue a value without waiting for one to arrive.
    ///
    /// # Errors
    ///
    /// Returns [`TryRecvError::Empty`] when no value is currently queued and the sender is still alive.
    /// Returns [`TryRecvError::Disconnected`] when no value is currently queued and the sender was dropped.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let Some(inner) = &self.inner else {
            return Err(TryRecvError::Disconnected);
        };
        let mut inner = inner.borrow_mut();
        if let Some(value) = inner.queue.pop_front() {
            inner.wake_sender();
            return Ok(value);
        }
        if inner.sender_alive {
            Err(TryRecvError::Empty)
        } else {
            Err(TryRecvError::Disconnected)
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
            inner.wake_sender();
        }
        drained
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
        inner.wake_sender();
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

impl<T> Receiver<T> {
    fn register_waker(&self, waker: &Waker) {
        if let Some(inner) = &self.inner {
            inner.borrow_mut().receiver_waker = Some(waker.clone());
        }
    }
}

impl<T> Inner<T> {
    fn wake_sender(&mut self) {
        if let Some(waker) = self.sender_waker.take() {
            waker.wake();
        }
    }

    fn wake_receiver(&mut self) {
        if let Some(waker) = self.receiver_waker.take() {
            waker.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future as _;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};

    use super::{TryRecvError, TrySendError, bounded};

    struct CountDrop {
        id: u32,
        drops: Arc<AtomicUsize>,
    }

    impl CountDrop {
        fn new(id: u32, drops: &Arc<AtomicUsize>) -> Self {
            Self {
                id,
                drops: Arc::clone(drops),
            }
        }
    }

    impl Drop for CountDrop {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test(flavor = "local")]
    async fn send_waits_for_receiver_capacity() {
        let (mut sender, mut receiver) = bounded(1);

        sender.try_send(1).unwrap();

        let producer = async {
            sender.send(2).await.unwrap();
        };
        let consumer = async {
            assert_eq!(receiver.recv().await, Ok(1));
            assert_eq!(receiver.recv().await, Ok(2));
        };

        tokio::join!(producer, consumer);
    }

    #[tokio::test(flavor = "local")]
    async fn recv_waits_for_sender_value() {
        let (mut sender, mut receiver) = bounded(1);

        let producer = async {
            sender.send(7).await.unwrap();
        };
        let consumer = async {
            assert_eq!(receiver.recv().await, Ok(7));
        };

        tokio::join!(producer, consumer);
    }

    #[tokio::test(flavor = "local")]
    async fn recv_observes_sender_disconnect() {
        let (sender, mut receiver) = bounded::<u32>(1);
        drop(sender);

        assert_eq!(receiver.recv().await, Err(TryRecvError::Disconnected));
    }

    #[tokio::test(flavor = "local")]
    async fn send_observes_receiver_disconnect() {
        let (mut sender, receiver) = bounded(1);
        drop(receiver);

        assert_eq!(sender.send(9).await, Err(TrySendError::Disconnected(9)));
    }

    #[tokio::test(flavor = "local")]
    async fn pending_recv_wakes_when_sender_drops() {
        let (sender, mut receiver) = bounded::<u32>(1);

        let consumer = async {
            assert_eq!(receiver.recv().await, Err(TryRecvError::Disconnected));
        };
        let producer = async {
            drop(sender);
        };

        tokio::join!(consumer, producer);
    }

    #[tokio::test(flavor = "local")]
    async fn pending_send_wakes_when_receiver_drops() {
        let (mut sender, receiver) = bounded(1);
        sender.try_send(1).unwrap();

        let producer = async {
            assert_eq!(sender.send(2).await, Err(TrySendError::Disconnected(2)));
        };
        let consumer = async {
            drop(receiver);
        };

        tokio::join!(producer, consumer);
    }

    #[test]
    fn dropped_pending_send_drops_value_once_and_does_not_enqueue() {
        let drops = Arc::new(AtomicUsize::new(0));
        let (mut sender, mut receiver) = bounded(1);
        assert!(sender.try_send(CountDrop::new(1, &drops)).is_ok());

        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        {
            let mut send = sender.send(CountDrop::new(2, &drops));
            assert!(matches!(Pin::new(&mut send).poll(&mut context), Poll::Pending));
        }

        assert_eq!(drops.load(Ordering::Relaxed), 1);

        let queued = receiver.try_recv().unwrap();
        assert_eq!(queued.id, 1);
        drop(queued);
        assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));
        assert_eq!(drops.load(Ordering::Relaxed), 2);

        assert!(sender.try_send(CountDrop::new(3, &drops)).is_ok());
        let queued = receiver.try_recv().unwrap();
        assert_eq!(queued.id, 3);
    }

    #[test]
    fn dropped_pending_recv_does_not_consume_later_value() {
        let (mut sender, mut receiver) = bounded(1);

        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        {
            let mut recv = receiver.recv();
            assert!(matches!(Pin::new(&mut recv).poll(&mut context), Poll::Pending));
        }

        sender.try_send(7).unwrap();

        assert_eq!(receiver.try_recv(), Ok(7));
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    }
}
