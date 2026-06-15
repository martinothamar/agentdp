use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

#[derive(Debug)]
pub struct Sender<T> {
    shared: Option<Rc<RefCell<Shared<T>>>>,
}

#[derive(Debug)]
pub struct Receiver<T> {
    shared: Rc<RefCell<Shared<T>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub T);

#[derive(Debug)]
struct Shared<T> {
    value: Option<T>,
    sender_alive: bool,
    receiver_alive: bool,
    receiver_waker: Option<Waker>,
}

#[must_use]
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Rc::new(RefCell::new(Shared {
        value: None,
        sender_alive: true,
        receiver_alive: true,
        receiver_waker: None,
    }));
    (
        Sender {
            shared: Some(Rc::clone(&shared)),
        },
        Receiver { shared },
    )
}

impl<T> Sender<T> {
    /// Sends the value to the receiver.
    ///
    /// # Errors
    ///
    /// Returns the original value when the receiver has already been dropped or
    /// this sender has already been consumed.
    pub fn send(mut self, value: T) -> Result<(), SendError<T>> {
        let Some(shared) = self.shared.take() else {
            return Err(SendError(value));
        };
        let mut shared = shared.borrow_mut();
        if !shared.receiver_alive {
            return Err(SendError(value));
        }
        shared.value = Some(value);
        shared.sender_alive = false;
        if let Some(waker) = shared.receiver_waker.take() {
            waker.wake();
        }
        Ok(())
    }

    /// Tries to send the value to the receiver, ignoring whether it succeeds or fails.
    pub fn try_send(self, value: T) {
        let _result = self.send(value);
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let Some(shared) = self.shared.take() else {
            return;
        };
        let mut shared = shared.borrow_mut();
        shared.sender_alive = false;
        if let Some(waker) = shared.receiver_waker.take() {
            waker.wake();
        }
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut shared = self.shared.borrow_mut();
        if let Some(value) = shared.value.take() {
            shared.receiver_alive = false;
            return Poll::Ready(Ok(value));
        }
        if !shared.sender_alive {
            shared.receiver_alive = false;
            return Poll::Ready(Err(RecvError));
        }
        shared.receiver_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.borrow_mut().receiver_alive = false;
    }
}

impl fmt::Display for RecvError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("oneshot sender dropped before sending")
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("oneshot receiver dropped before send")
    }
}

impl std::error::Error for RecvError {}

impl<T: fmt::Debug> std::error::Error for SendError<T> {}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use super::{RecvError, SendError, channel};

    #[tokio::test(flavor = "local")]
    async fn receive_value() {
        let (sender, receiver) = channel();

        sender.send(42).unwrap();

        assert_eq!(receiver.await, Ok(42));
    }

    #[tokio::test(flavor = "local")]
    async fn receiver_observes_sender_drop() {
        let (sender, receiver) = channel::<u32>();

        drop(sender);

        assert_eq!(receiver.await, Err(RecvError));
    }

    #[test]
    fn sender_observes_receiver_drop() {
        let (sender, receiver) = channel();

        drop(receiver);

        assert_eq!(sender.send(42), Err(SendError(42)));
    }

    #[tokio::test(flavor = "local")]
    async fn sent_value_drops_once_when_receiver_drops() {
        #[derive(Debug)]
        struct CountDrop(Rc<Cell<u32>>);

        impl Drop for CountDrop {
            fn drop(&mut self) {
                self.0.set(self.0.get() + 1);
            }
        }

        let drops = Rc::new(Cell::new(0));
        let (sender, receiver) = channel();

        sender.send(CountDrop(Rc::clone(&drops))).unwrap();
        drop(receiver);

        assert_eq!(drops.get(), 1);
    }
}
