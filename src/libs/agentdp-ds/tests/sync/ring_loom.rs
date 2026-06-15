#![cfg(feature = "loom")]

use agentdp_ds::sync::ring::{Consumer, Producer, TryReadError, TryReserveError, bounded, buffered};
use loom::thread;

#[test]
fn capacity_one_reserve_commit_read_handoff() {
    loom::model(|| {
        let (mut producer, mut consumer) = buffered::<u32>(1, 1);

        let producer = thread::spawn(move || {
            producer.write_with(|value| *value = 1).unwrap();
            producer.flush();
        });
        let consumer = thread::spawn(move || {
            assert_eq!(read_one_until_value(&mut consumer), 1);
            assert_eq!(read_until_disconnect(&mut consumer), TryReadError::Disconnected);
        });

        assert!(producer.join().is_ok());
        assert!(consumer.join().is_ok());
    });
}

#[test]
fn capacity_two_preserves_fifo_across_threads() {
    loom::model(|| {
        let (mut producer, mut consumer) = bounded::<u32>(2);

        let producer = thread::spawn(move || {
            reserve_until_committed(&mut producer, 1);
            reserve_until_committed(&mut producer, 2);
        });
        let consumer = thread::spawn(move || {
            assert_eq!(read_one_until_value(&mut consumer), 1);
            assert_eq!(read_one_until_value(&mut consumer), 2);
        });

        assert!(producer.join().is_ok());
        assert!(consumer.join().is_ok());
    });
}

#[test]
fn full_ring_observes_consumer_release() {
    loom::model(|| {
        let (mut producer, mut consumer) = bounded::<u32>(1);
        reserve_until_committed(&mut producer, 1);

        let producer = thread::spawn(move || {
            reserve_until_committed(&mut producer, 2);
        });
        let consumer = thread::spawn(move || {
            assert_eq!(read_one_until_value(&mut consumer), 1);
            assert_eq!(read_one_until_value(&mut consumer), 2);
        });

        assert!(producer.join().is_ok());
        assert!(consumer.join().is_ok());
    });
}

#[test]
fn batch_commit_is_observed_in_order() {
    loom::model(|| {
        let (mut producer, mut consumer) = bounded::<u32>(2);

        let producer = thread::spawn(move || {
            let mut batch = producer.try_reserve_batch(2).unwrap();
            batch.fill(|index, value| *value = index as u32 + 1);
            batch.commit();
        });
        let consumer = thread::spawn(move || {
            let mut values = Vec::new();
            while values.len() < 2 {
                match consumer.try_read_batch(2) {
                    Ok(batch) => {
                        batch.for_each(|_index, value| values.push(*value));
                    }
                    Err(TryReadError::Empty) => thread::yield_now(),
                    Err(TryReadError::Disconnected) => unreachable!("producer should publish before disconnect"),
                }
            }
            assert_eq!(values, vec![1, 2]);
        });

        assert!(producer.join().is_ok());
        assert!(consumer.join().is_ok());
    });
}

#[test]
fn buffered_batch_is_observed_after_flush() {
    loom::model(|| {
        let (mut producer, mut consumer) = buffered::<u32>(2, 2);

        let producer = thread::spawn(move || {
            producer.write_with(|value| *value = 1).unwrap();
            producer.write_with(|value| *value = 2).unwrap();
        });
        let consumer = thread::spawn(move || {
            let mut values = Vec::new();
            while values.len() < 2 {
                match consumer.try_read_batch(2) {
                    Ok(batch) => {
                        batch.for_each(|_index, value| values.push(*value));
                    }
                    Err(TryReadError::Empty) => thread::yield_now(),
                    Err(TryReadError::Disconnected) => unreachable!("producer should publish before disconnect"),
                }
            }
            assert_eq!(values, vec![1, 2]);
        });

        assert!(producer.join().is_ok());
        assert!(consumer.join().is_ok());
    });
}

#[test]
fn producer_drop_is_observed_after_buffer_drains() {
    loom::model(|| {
        let (mut producer, mut consumer) = bounded::<u32>(2);

        let producer = thread::spawn(move || {
            reserve_until_committed(&mut producer, 1);
            reserve_until_committed(&mut producer, 2);
        });
        let consumer = thread::spawn(move || {
            assert_eq!(read_one_until_value(&mut consumer), 1);
            assert_eq!(read_one_until_value(&mut consumer), 2);
            assert_eq!(read_until_disconnect(&mut consumer), TryReadError::Disconnected);
        });

        assert!(producer.join().is_ok());
        assert!(consumer.join().is_ok());
    });
}

#[test]
fn consumer_drop_is_observed_without_publishing_reserved_slot() {
    loom::model(|| {
        let (mut producer, consumer) = bounded::<u32>(1);

        let producer = thread::spawn(move || {
            loop {
                match producer.try_reserve_batch(1) {
                    Ok(_batch) => thread::yield_now(),
                    Err(TryReserveError::Full) => thread::yield_now(),
                    Err(TryReserveError::Disconnected) => return,
                }
            }
        });
        let consumer = thread::spawn(move || drop(consumer));

        assert!(producer.join().is_ok());
        assert!(consumer.join().is_ok());
    });
}

fn reserve_until_committed(producer: &mut Producer<u32>, value: u32) {
    loop {
        match producer.try_reserve_batch(1) {
            Ok(mut batch) => {
                batch.fill(|_index, target| *target = value);
                batch.commit();
                return;
            }
            Err(TryReserveError::Full) => thread::yield_now(),
            Err(TryReserveError::Disconnected) => unreachable!("consumer should stay connected"),
        }
    }
}

fn read_one_until_value(consumer: &mut Consumer<u32>) -> u32 {
    loop {
        match consumer.try_read_batch(1) {
            Ok(batch) => {
                let mut value = None;
                batch.for_each(|_index, next| value = Some(*next));
                return value.expect("batch should contain one value");
            }
            Err(TryReadError::Empty) => thread::yield_now(),
            Err(TryReadError::Disconnected) => unreachable!("producer should stay connected"),
        }
    }
}

fn read_until_disconnect(consumer: &mut Consumer<u32>) -> TryReadError {
    loop {
        match consumer.try_read_batch(1) {
            Ok(_batch) => thread::yield_now(),
            Err(TryReadError::Empty) => thread::yield_now(),
            Err(error @ TryReadError::Disconnected) => return error,
        }
    }
}
