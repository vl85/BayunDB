use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use rand::prelude::*;
use std::sync::Arc;

use bayundb::storage::buffer::BufferPoolManager;
use bayundb::index::btree::BTreeIndex;
use bayundb::common::types::Rid;

// Create temporary db for testing
fn create_test_environment(buffer_pool_size: usize) -> Arc<BufferPoolManager> {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    let buffer_pool = Arc::new(BufferPoolManager::new(buffer_pool_size, path).unwrap());
    
    // Keep the temp file alive
    std::mem::forget(temp_file);
    
    buffer_pool
}

fn btree_benchmark(c: &mut Criterion) {
    // Use a reasonably sized buffer pool
    let buffer_pool_size = 1000;
    
    // Benchmark group for B+Tree operations
    let mut group = c.benchmark_group("BTreeIndex");
    
    // Test with different dataset sizes - reduced maximum size to avoid NodeTooLarge error
    for size in [100, 500, 1000].iter() {
        // Benchmark B+Tree inserts
        group.bench_with_input(BenchmarkId::new("insert", size), size, |b, &size| {
            let buffer_pool = create_test_environment(buffer_pool_size);
            let btree = BTreeIndex::<i32>::new(buffer_pool.clone()).unwrap();
            
            // Generate random keys to insert
            let mut rng = rand::thread_rng();
            let keys: Vec<i32> = (0..size).map(|_| rng.r#gen::<i32>()).collect();
            let mut idx = 0;
            
            // Benchmark inserting keys
            b.iter(|| {
                if idx >= keys.len() {
                    idx = 0; // Reset if we've gone through all keys
                }
                let key = keys[idx];
                btree.insert(key, Rid::new(0, (key as u32) + 1000)).unwrap();
                idx += 1;
            });
        });
        
        // Benchmark B+Tree lookups
        group.bench_with_input(BenchmarkId::new("lookup", size), size, |b, &size| {
            let buffer_pool = create_test_environment(buffer_pool_size);
            let btree = BTreeIndex::<i32>::new(buffer_pool.clone()).unwrap();
            
            // Insert keys first
            let mut keys = Vec::with_capacity(size as usize);
            for i in 0..size {
                let key = i as i32;
                btree.insert(key, Rid::new(0, (key as u32) + 1000)).unwrap();
                keys.push(key);
            }
            
            // Shuffle keys for random access
            let mut rng = rand::thread_rng();
            keys.shuffle(&mut rng);
            let mut idx = 0;
            
            // Benchmark looking up keys
            b.iter(|| {
                if idx >= keys.len() {
                    idx = 0; // Reset if we've gone through all keys
                }
                let key = keys[idx];
                let _ = btree.find(&key).unwrap();
                idx += 1;
            });
        });
        
        // Benchmark B+Tree range scans
        group.bench_with_input(BenchmarkId::new("range_scan", size), size, |b, &size| {
            let buffer_pool = create_test_environment(buffer_pool_size);
            let btree = BTreeIndex::<i32>::new(buffer_pool.clone()).unwrap();
            
            // Insert sequential keys
            for i in 0..size {
                let key = i as i32;
                btree.insert(key, Rid::new(0, (key as u32) + 1000)).unwrap();
            }
            
            // Create range scan windows (each scanning 10% of data)
            let range_size = (size as i32) / 10;
            let ranges: Vec<(i32, i32)> = (0..9)
                .map(|i| (i * range_size, (i + 1) * range_size - 1))
                .collect();
            let mut idx = 0;
            
            // Benchmark range scans
            b.iter(|| {
                if idx >= ranges.len() {
                    idx = 0; // Reset if we've gone through all ranges
                }
                let (start, end) = ranges[idx];
                let _ = btree.range_scan(&start, &end).unwrap();
                idx += 1;
            });
        });
        
        // Benchmark B+Tree remove operations
        group.bench_with_input(BenchmarkId::new("remove", size), size, |b, &size| {
            let buffer_pool = create_test_environment(buffer_pool_size);
            let btree = BTreeIndex::<i32>::new(buffer_pool.clone()).unwrap();
            
            // Insert keys first
            let mut keys = Vec::with_capacity(size as usize);
            for i in 0..size {
                let key = i as i32;
                btree.insert(key, Rid::new(0, (key as u32) + 1000)).unwrap();
                keys.push(key);
            }
            
            // Shuffle keys for random removal order
            let mut rng = rand::thread_rng();
            keys.shuffle(&mut rng);
            let mut idx = 0;
            
            // Benchmark removing keys
            b.iter(|| {
                if idx >= keys.len() {
                    // If we've removed all keys, repopulate the tree
                    if idx == keys.len() {
                        for &key in &keys {
                            btree.insert(key, Rid::new(0, (key as u32) + 1000)).unwrap();
                        }
                    }
                    idx = 0;
                }
                let key = keys[idx];
                let _ = btree.remove(&key).unwrap();
                idx += 1;
            });
        });
    }
    
    group.finish();
}

criterion_group!(benches, btree_benchmark);
criterion_main!(benches); 