pub mod mod_rs;
pub mod basic_operations;
pub mod embedded_api;

use criterion::{criterion_group, criterion_main};

criterion_group!(
    benches, 
    basic_operations::benchmark_create_and_populate,
    basic_operations::benchmark_select_queries,
    embedded_api::benchmark_embedded_api
);
criterion_main!(benches); 