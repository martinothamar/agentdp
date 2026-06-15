#![cfg(feature = "loom")]

use agentdp_ds::sync::spsc::{TryRecvError, TrySendError, bounded};
use loom::sync::Arc;
use loom::sync::Mutex;
use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use loom::thread;

#[test]
fn capacity_one_send_recv_handoff() {
    loom::model(|| {
        let (mut sender, mut receiver) = bounded(1);

        let producer = thread::spawn(move || {
            assert_eq!(sender.try_send(1), Ok(()));
        });
        let consumer = thread::spawn(move || {
            assert_eq!(recv_until_value(&mut receiver), 1);
            assert_eq!(recv_until_disconnect(&mut receiver), TryRecvError::Disconnected);
        });

        assert!(producer.join().is_ok());
        assert!(consumer.join().is_ok());
    });
}

#[test]
fn capacity_two_preserves_fifo_across_threads() {
    loom::model(|| {
        let (mut sender, mut receiver) = bounded(2);

        let producer = thread::spawn(move || {
            assert_eq!(sender.try_send(1), Ok(()));
            send_until_accepted(&mut sender, 2);
        });
        let consumer = thread::spawn(move || {
            assert_eq!(recv_until_value(&mut receiver), 1);
            assert_eq!(recv_until_value(&mut receiver), 2);
        });

        assert!(producer.join().is_ok());
        assert!(consumer.join().is_ok());
    });
}

#[test]
fn full_queue_observes_consumer_progress() {
    loom::model(|| {
        let (mut sender, mut receiver) = bounded(1);
        assert_eq!(sender.try_send(1), Ok(()));

        let producer = thread::spawn(move || {
            send_until_accepted(&mut sender, 2);
        });
        let consumer = thread::spawn(move || {
            assert_eq!(recv_until_value(&mut receiver), 1);
            assert_eq!(recv_until_value(&mut receiver), 2);
        });

        assert!(producer.join().is_ok());
        assert!(consumer.join().is_ok());
    });
}

#[test]
fn sender_drop_is_observed_after_queued_values_drain() {
    loom::model(|| {
        let (mut sender, mut receiver) = bounded(2);

        let producer = thread::spawn(move || {
            assert_eq!(sender.try_send(1), Ok(()));
            assert_eq!(sender.try_send(2), Ok(()));
        });
        let consumer = thread::spawn(move || {
            assert_eq!(recv_until_value(&mut receiver), 1);
            assert_eq!(recv_until_value(&mut receiver), 2);
            assert_eq!(recv_until_disconnect(&mut receiver), TryRecvError::Disconnected);
        });

        assert!(producer.join().is_ok());
        assert!(consumer.join().is_ok());
    });
}

#[test]
fn receiver_drop_is_observed_without_publishing_value() {
    loom::model(|| {
        let (mut sender, receiver) = bounded(1);

        let producer = thread::spawn(move || {
            assert_eq!(send_until_disconnected(&mut sender, 1), TrySendError::Disconnected(1));
        });
        let consumer = thread::spawn(move || {
            drop(receiver);
        });

        assert!(producer.join().is_ok());
        assert!(consumer.join().is_ok());
    });
}

#[test]
fn queued_values_drop_once_when_channel_is_dropped() {
    loom::model(|| {
        struct CountDrop(Arc<AtomicUsize>);

        impl Drop for CountDrop {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let (mut sender, receiver) = bounded(2);
        assert!(sender.try_send(CountDrop(Arc::clone(&drops))).is_ok());
        assert!(sender.try_send(CountDrop(Arc::clone(&drops))).is_ok());

        let producer = thread::spawn(move || {
            drop(sender);
        });
        let consumer = thread::spawn(move || {
            drop(receiver);
        });

        assert!(producer.join().is_ok());
        assert!(consumer.join().is_ok());
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    });
}

#[test]
fn async_send_register_retry_protocol_does_not_lose_capacity_wake() {
    loom::model(|| {
        let state = Arc::new(NotifyProtocol::new());

        let sender_state = Arc::clone(&state);
        let sender = thread::spawn(move || {
            if sender_state.ready.load(Ordering::Acquire) {
                sender_state.completed.store(true, Ordering::Release);
                return;
            }

            *lock(&sender_state.registered) = true;
            if sender_state.ready.load(Ordering::Acquire) {
                sender_state.completed.store(true, Ordering::Release);
            } else {
                sender_state.pending.store(true, Ordering::Release);
            }
        });

        let receiver_state = Arc::clone(&state);
        let receiver = thread::spawn(move || {
            receiver_state.ready.store(true, Ordering::Release);
            if *lock(&receiver_state.registered) {
                receiver_state.woken.store(true, Ordering::Release);
            }
        });

        assert!(sender.join().is_ok());
        assert!(receiver.join().is_ok());
        assert_not_lost_wake(&state);
    });
}

#[test]
fn async_recv_register_retry_protocol_does_not_lose_value_wake() {
    loom::model(|| {
        let state = Arc::new(NotifyProtocol::new());

        let receiver_state = Arc::clone(&state);
        let receiver = thread::spawn(move || {
            if receiver_state.ready.load(Ordering::Acquire) {
                receiver_state.completed.store(true, Ordering::Release);
                return;
            }

            *lock(&receiver_state.registered) = true;
            if receiver_state.ready.load(Ordering::Acquire) {
                receiver_state.completed.store(true, Ordering::Release);
            } else {
                receiver_state.pending.store(true, Ordering::Release);
            }
        });

        let sender_state = Arc::clone(&state);
        let sender = thread::spawn(move || {
            sender_state.ready.store(true, Ordering::Release);
            if *lock(&sender_state.registered) {
                sender_state.woken.store(true, Ordering::Release);
            }
        });

        assert!(receiver.join().is_ok());
        assert!(sender.join().is_ok());
        assert_not_lost_wake(&state);
    });
}

struct NotifyProtocol {
    ready: AtomicBool,
    registered: Mutex<bool>,
    pending: AtomicBool,
    completed: AtomicBool,
    woken: AtomicBool,
}

impl NotifyProtocol {
    fn new() -> Self {
        Self {
            ready: AtomicBool::new(false),
            registered: Mutex::new(false),
            pending: AtomicBool::new(false),
            completed: AtomicBool::new(false),
            woken: AtomicBool::new(false),
        }
    }
}

fn assert_not_lost_wake(state: &NotifyProtocol) {
    let pending = state.pending.load(Ordering::Acquire);
    let ready = state.ready.load(Ordering::Acquire);
    let completed = state.completed.load(Ordering::Acquire);
    let woken = state.woken.load(Ordering::Acquire);

    assert!(
        !pending || woken,
        "operation went pending after registering, but readiness did not wake it"
    );
    assert!(
        pending || completed,
        "operation must either complete on retry or park waiting for a wake"
    );
    assert!(ready, "peer must publish readiness before the model ends");
}

fn send_until_accepted(sender: &mut agentdp_ds::sync::spsc::Sender<u32>, mut value: u32) {
    loop {
        match sender.try_send(value) {
            Ok(()) => return,
            Err(TrySendError::Full(next)) => {
                value = next;
                thread::yield_now();
            }
            Err(TrySendError::Disconnected(_)) => unreachable!("receiver should stay connected"),
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> loom::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn send_until_disconnected(sender: &mut agentdp_ds::sync::spsc::Sender<u32>, mut value: u32) -> TrySendError<u32> {
    loop {
        match sender.try_send(value) {
            Ok(()) => thread::yield_now(),
            Err(TrySendError::Full(next)) => {
                value = next;
                thread::yield_now();
            }
            Err(error @ TrySendError::Disconnected(_)) => return error,
        }
    }
}

fn recv_until_value(receiver: &mut agentdp_ds::sync::spsc::Receiver<u32>) -> u32 {
    loop {
        match receiver.try_recv() {
            Ok(value) => return value,
            Err(TryRecvError::Empty) => thread::yield_now(),
            Err(TryRecvError::Disconnected) => unreachable!("sender should stay connected"),
        }
    }
}

fn recv_until_disconnect(receiver: &mut agentdp_ds::sync::spsc::Receiver<u32>) -> TryRecvError {
    loop {
        match receiver.try_recv() {
            Ok(_) | Err(TryRecvError::Empty) => thread::yield_now(),
            Err(error @ TryRecvError::Disconnected) => return error,
        }
    }
}
