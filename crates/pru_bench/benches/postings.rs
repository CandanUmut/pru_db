use criterion::{criterion_group, criterion_main, Criterion, black_box};
use pru_core::postings::{encode_sorted_u64, decode_sorted_u64, intersect_sorted};

fn bench_postings(c: &mut Criterion) {
    let a: Vec<u64> = (0..100_000).step_by(2).map(|x| x as u64).collect();
    let b: Vec<u64> = (0..100_000).step_by(3).map(|x| x as u64).collect();
    let enc = encode_sorted_u64(&a);
    c.bench_function("encode", |bch| bch.iter(|| black_box(encode_sorted_u64(&a))));
    c.bench_function("decode", |bch| bch.iter(|| black_box(decode_sorted_u64(&enc))));
    c.bench_function("intersect", |bch| bch.iter(|| black_box(intersect_sorted(&a, &b))));
}

criterion_group!(benches, bench_postings);
criterion_main!(benches);
