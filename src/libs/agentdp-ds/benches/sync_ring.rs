use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use std::{hint, thread};

use agentdp_ds::sync::ring;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

const CAPACITY: usize = 1024;
const BATCH_SIZE: usize = 32;

#[derive(Debug, Clone, Copy, Default)]
struct Event {
    sequence: u64,
    payload: u64,
}

fn network_event_transfer(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_ring_network_event_transfer");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Elements(1));
    group.bench_function("agentdp-ds-batch-32", |b| {
        b.iter_custom(sync_ring_network_event_transfer);
    });
    group.finish();
}

const fn event(index: u64) -> Event {
    Event {
        sequence: index,
        payload: index.wrapping_mul(31),
    }
}

fn sync_ring_network_event_transfer(iterations: u64) -> Duration {
    let (mut producer, mut consumer) = ring::buffered::<Event>(CAPACITY, BATCH_SIZE);
    let barrier = Arc::new(Barrier::new(2));
    let consumer_barrier = Arc::clone(&barrier);
    let consumer = thread::spawn(move || {
        consumer_barrier.wait();
        let mut received = 0_u64;
        while received < iterations {
            match consumer.try_read_batch(BATCH_SIZE) {
                Ok(batch) => {
                    batch.for_each(|_index, event| {
                        black_box(event.sequence);
                        black_box(event.payload);
                    });
                    received = received.saturating_add(batch.len() as u64);
                }
                Err(ring::TryReadError::Empty) => hint::spin_loop(),
                Err(ring::TryReadError::Disconnected) => std::process::abort(),
            }
        }
        black_box(received);
    });

    barrier.wait();
    let started = Instant::now();
    for index in 0..iterations {
        loop {
            match producer.write_with(|slot| *slot = event(index)) {
                Ok(()) => break,
                Err(ring::TryReserveError::Full) => hint::spin_loop(),
                Err(ring::TryReserveError::Disconnected) => std::process::abort(),
            }
        }
    }
    producer.flush();
    if consumer.join().is_err() {
        std::process::abort();
    }
    started.elapsed()
}

criterion_group!(benches, network_event_transfer);
criterion_main!(benches);
