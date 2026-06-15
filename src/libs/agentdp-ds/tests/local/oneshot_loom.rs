#![cfg(feature = "loom")]

use std::future::Future as _;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use agentdp_ds::local::oneshot::{RecvError, SendError, channel};

// The local oneshot is intentionally Rc<RefCell>-backed, so these models do not
// spawn threads. They keep the future/waker state transitions covered under the
// same loom gate as the lock-free ds primitives without changing the type's
// single-thread contract. Wakers also require std::sync::Arc through std::task.

#[test]
fn receiver_completes_when_value_is_sent_before_poll() {
    loom::model(|| {
        let (sender, mut receiver) = channel();
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = count_waker(&wakes);
        let mut context = Context::from_waker(&waker);

        sender.send(42).unwrap();

        assert_eq!(wakes.load(Ordering::Relaxed), 0);
        assert_eq!(Pin::new(&mut receiver).poll(&mut context), Poll::Ready(Ok(42)));
    });
}

#[test]
fn pending_receiver_is_woken_by_send() {
    loom::model(|| {
        let (sender, mut receiver) = channel();
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = count_waker(&wakes);
        let mut context = Context::from_waker(&waker);

        assert_eq!(Pin::new(&mut receiver).poll(&mut context), Poll::Pending);

        sender.send(42).unwrap();

        assert_eq!(wakes.load(Ordering::Relaxed), 1);
        assert_eq!(Pin::new(&mut receiver).poll(&mut context), Poll::Ready(Ok(42)));
    });
}

#[test]
fn second_pending_poll_replaces_receiver_waker() {
    loom::model(|| {
        let (sender, mut receiver) = channel();
        let first_wakes = Arc::new(AtomicUsize::new(0));
        let second_wakes = Arc::new(AtomicUsize::new(0));
        let first_waker = count_waker(&first_wakes);
        let second_waker = count_waker(&second_wakes);
        let mut first_context = Context::from_waker(&first_waker);
        let mut second_context = Context::from_waker(&second_waker);

        assert_eq!(Pin::new(&mut receiver).poll(&mut first_context), Poll::Pending);
        assert_eq!(Pin::new(&mut receiver).poll(&mut second_context), Poll::Pending);

        sender.send(42).unwrap();

        assert_eq!(first_wakes.load(Ordering::Relaxed), 0);
        assert_eq!(second_wakes.load(Ordering::Relaxed), 1);
        assert_eq!(Pin::new(&mut receiver).poll(&mut second_context), Poll::Ready(Ok(42)));
    });
}

#[test]
fn pending_receiver_is_woken_by_sender_drop() {
    loom::model(|| {
        let (sender, mut receiver) = channel::<u32>();
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = count_waker(&wakes);
        let mut context = Context::from_waker(&waker);

        assert_eq!(Pin::new(&mut receiver).poll(&mut context), Poll::Pending);

        drop(sender);

        assert_eq!(wakes.load(Ordering::Relaxed), 1);
        assert_eq!(Pin::new(&mut receiver).poll(&mut context), Poll::Ready(Err(RecvError)));
    });
}

#[test]
fn receiver_drop_after_pending_makes_send_fail_without_wake() {
    loom::model(|| {
        let (sender, mut receiver) = channel();
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = count_waker(&wakes);
        let mut context = Context::from_waker(&waker);

        assert_eq!(Pin::new(&mut receiver).poll(&mut context), Poll::Pending);
        drop(receiver);

        assert_eq!(sender.send(42), Err(SendError(42)));
        assert_eq!(wakes.load(Ordering::Relaxed), 0);
    });
}

#[test]
fn sender_observes_receiver_drop() {
    loom::model(|| {
        let (sender, receiver) = channel();

        drop(receiver);

        assert_eq!(sender.send(42), Err(SendError(42)));
    });
}

#[test]
fn sent_value_drops_once_when_receiver_drops() {
    loom::model(|| {
        #[derive(Debug)]
        struct CountDrop(Arc<AtomicUsize>);

        impl Drop for CountDrop {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let (sender, receiver) = channel();

        sender.send(CountDrop(Arc::clone(&drops))).unwrap();
        drop(receiver);

        assert_eq!(drops.load(Ordering::Relaxed), 1);
    });
}

struct CountWake {
    wakes: Arc<AtomicUsize>,
}

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::Relaxed);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::Relaxed);
    }
}

fn count_waker(wakes: &Arc<AtomicUsize>) -> Waker {
    Waker::from(Arc::new(CountWake {
        wakes: Arc::clone(wakes),
    }))
}
