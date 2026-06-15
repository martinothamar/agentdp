use std::hint::black_box;
use std::sync::mpsc;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use std::{hint, thread};

use agentdp_ds::sync::spsc;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

const CAPACITY: usize = 1024;

#[derive(Debug, Clone, Copy)]
struct Event {
    sequence: u64,
    payload: u64,
}

fn network_event_transfer(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_network_event_transfer");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Elements(1));
    group.bench_function("agentdp-ds", |b| {
        b.iter_custom(spsc_network_event_transfer);
    });
    group.bench_function("std-sync-mpsc", |b| {
        b.iter_custom(mpsc_network_event_transfer);
    });
    group.finish();
}

const fn event(index: u64) -> Event {
    Event {
        sequence: index,
        payload: index.wrapping_mul(31),
    }
}

fn spsc_network_event_transfer(iterations: u64) -> Duration {
    let (mut sender, mut receiver) = spsc::bounded(CAPACITY);
    let barrier = Arc::new(Barrier::new(2));
    let consumer_barrier = Arc::clone(&barrier);
    let consumer = thread::spawn(move || {
        consumer_barrier.wait();
        let mut consumed = 0_u64;
        while consumed < iterations {
            let drained = receiver.drain(|event: Event| {
                black_box(event.sequence);
                black_box(event.payload);
            });
            if drained == 0 {
                hint::spin_loop();
            }
            consumed = consumed.saturating_add(drained as u64);
        }
        black_box(consumed);
    });

    barrier.wait();
    let started = Instant::now();
    for index in 0..iterations {
        let mut next = event(index);
        loop {
            match sender.try_send(next) {
                Ok(()) => break,
                Err(spsc::TrySendError::Full(value)) => {
                    next = value;
                    hint::spin_loop();
                }
                Err(spsc::TrySendError::Disconnected(_value)) => std::process::abort(),
            }
        }
    }
    if consumer.join().is_err() {
        std::process::abort();
    }
    started.elapsed()
}

fn mpsc_network_event_transfer(iterations: u64) -> Duration {
    let (sender, receiver) = mpsc::sync_channel(CAPACITY);
    let barrier = Arc::new(Barrier::new(2));
    let consumer_barrier = Arc::clone(&barrier);
    let consumer = thread::spawn(move || {
        consumer_barrier.wait();
        for _ in 0..iterations {
            loop {
                match receiver.try_recv() {
                    Ok(event) => {
                        black_box(event);
                        break;
                    }
                    Err(mpsc::TryRecvError::Empty) => hint::spin_loop(),
                    Err(mpsc::TryRecvError::Disconnected) => std::process::abort(),
                }
            }
        }
    });

    barrier.wait();
    let started = Instant::now();
    for index in 0..iterations {
        let mut next = event(index);
        loop {
            match sender.try_send(next) {
                Ok(()) => break,
                Err(mpsc::TrySendError::Full(value)) => {
                    next = value;
                    hint::spin_loop();
                }
                Err(mpsc::TrySendError::Disconnected(_value)) => std::process::abort(),
            }
        }
    }
    if consumer.join().is_err() {
        std::process::abort();
    }
    started.elapsed()
}

criterion_group!(benches, network_event_transfer);
criterion_main!(benches);
