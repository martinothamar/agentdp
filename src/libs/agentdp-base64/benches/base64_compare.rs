use std::hint::black_box;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use agentdp_base64::{decode, decoded_len, encode, encoded_len};

const BENCH_SIZES: [usize; 6] = [16, 32, 64, 128, 256, 1024];

fn build_payload(size: usize) -> Vec<u8> {
    let mut state = 0x1234_5678_u64;
    let mut payload = Vec::with_capacity(size);

    while payload.len() < size {
        state ^= state.rotate_left(13);
        state ^= state >> 7;
        state ^= state << 17;
        payload.push(state.to_be_bytes()[0]);
    }

    payload
}

fn encode_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode");

    for &size in &BENCH_SIZES {
        let input = build_payload(size);
        let mut our_output = vec![0u8; encoded_len(size)];
        let mut base_output = vec![0u8; encoded_len(size)];
        let base_input = input.clone();

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("agentdp-base64", size), &size, move |b, _| {
            b.iter(|| {
                if let Some(written) = encode(black_box(&input), black_box(&mut our_output)) {
                    black_box(written);
                    black_box(&our_output);
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("base64-crate", size), &size, move |b, _| {
            b.iter(|| {
                if let Ok(written) = STANDARD.encode_slice(black_box(&base_input), black_box(&mut base_output)) {
                    black_box(written);
                    black_box(&base_output);
                }
            });
        });
    }

    group.finish();
}

fn decode_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode");

    for &size in &BENCH_SIZES {
        let input = build_payload(size);
        let mut encoded = vec![0u8; encoded_len(size)];
        let Some(encode_written) = encode(&input, &mut encoded) else {
            continue;
        };
        encoded.truncate(encode_written);
        let baseline_encoded = encoded.clone();

        let Some(decoded_len) = decoded_len(&encoded) else {
            continue;
        };
        let mut our_output = vec![0u8; decoded_len];
        let mut base_output = vec![0u8; decoded_len];

        group.throughput(Throughput::Bytes(encoded.len() as u64));
        group.bench_with_input(BenchmarkId::new("agentdp-base64", size), &size, move |b, _| {
            b.iter(|| {
                if let Some(written) = decode(black_box(&encoded), black_box(&mut our_output)) {
                    black_box(written);
                    black_box(&our_output);
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("base64-crate", size), &size, move |b, _| {
            b.iter(|| {
                if let Ok(written) = STANDARD.decode_slice(black_box(&baseline_encoded), black_box(&mut base_output)) {
                    black_box(written);
                    black_box(&base_output);
                }
            });
        });
    }

    group.finish();
}

criterion_group!(benches, encode_bench, decode_bench);
criterion_main!(benches);
