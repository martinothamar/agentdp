use std::hint::black_box;
use std::time::{Duration, Instant};

use agentdp_ds::local::spsc;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

const CAPACITY: usize = 1024;

#[derive(Debug, Clone, Copy)]
struct Event {
    sequence: u64,
    payload: u64,
}

fn network_event_transfer(c: &mut Criterion) {
    let runtime = match tokio::runtime::Builder::new_current_thread().enable_time().build() {
        Ok(runtime) => runtime,
        Err(_error) => std::process::abort(),
    };
    let local = tokio::task::LocalSet::new();

    let mut group = c.benchmark_group("local_spsc_network_event_transfer");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Elements(1));
    group.bench_function("agentdp-ds", |b| {
        b.iter_custom(|iterations| local.block_on(&runtime, local_spsc_network_event_transfer(iterations)));
    });
    group.bench_function("tokio-mpsc", |b| {
        b.iter_custom(|iterations| local.block_on(&runtime, tokio_mpsc_network_event_transfer(iterations)));
    });
    group.finish();
}

const fn event(index: u64) -> Event {
    Event {
        sequence: index,
        payload: index.wrapping_mul(31),
    }
}

#[allow(
    clippy::future_not_send,
    reason = "benchmark intentionally exercises the local Rc-backed SPSC queue on a current-thread runtime"
)]
async fn local_spsc_network_event_transfer(iterations: u64) -> Duration {
    let (mut sender, mut receiver) = spsc::bounded::<Event>(CAPACITY);
    let consumer = tokio::task::spawn_local(async move {
        for _ in 0..iterations {
            match receiver.recv().await {
                Ok(event) => {
                    black_box(event.sequence);
                    black_box(event.payload);
                }
                Err(spsc::TryRecvError::Empty | spsc::TryRecvError::Disconnected) => {
                    std::process::abort();
                }
            }
        }
    });

    let started = Instant::now();
    for index in 0..iterations {
        if sender.send(event(index)).await.is_err() {
            std::process::abort();
        }
    }
    if consumer.await.is_err() {
        std::process::abort();
    }
    started.elapsed()
}

async fn tokio_mpsc_network_event_transfer(iterations: u64) -> Duration {
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Event>(CAPACITY);
    let consumer = tokio::task::spawn_local(async move {
        for _ in 0..iterations {
            let Some(event) = receiver.recv().await else {
                std::process::abort();
            };
            black_box(event.sequence);
            black_box(event.payload);
        }
    });

    let started = Instant::now();
    for index in 0..iterations {
        if sender.send(event(index)).await.is_err() {
            std::process::abort();
        }
    }
    if consumer.await.is_err() {
        std::process::abort();
    }
    started.elapsed()
}

criterion_group!(benches, network_event_transfer);
criterion_main!(benches);
