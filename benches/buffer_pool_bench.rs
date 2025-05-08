use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use rand::prelude::*;

use std::sync::Arc;
use bayundb::storage::buffer::BufferPoolManager;
use bayundb::storage::page::PageManager;

// Create temporary db for testing
fn create_test_buffer_pool(pool_size: usize) -> Arc<BufferPoolManager> {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    let buffer_pool = Arc::new(BufferPoolManager::new(pool_size, path).unwrap());
    
    // Keep the temp file alive
    std::mem::forget(temp_file);
    
    buffer_pool
}

// Generate test data of specified size
fn generate_test_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

fn buffer_pool_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("BufferPool");
    
    // Test with different buffer pool sizes
    for size in [10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::new("sequential_access", size), size, |b, &size| {
            let buffer_pool = create_test_buffer_pool(size as usize);
            let page_manager = PageManager::new();
            
            // Create some pages first
            let mut page_ids = Vec::new();
            for _ in 0..size {
                let (page, page_id) = buffer_pool.new_page().unwrap();
                
                // Initialize the page
                {
                    let mut page_guard = page.write();
                    page_manager.init_page(&mut page_guard);
                    
                    // Add some data
                    let data = generate_test_data(100);
                    page_manager.insert_record(&mut page_guard, &data).unwrap();
                }
                
                buffer_pool.unpin_page(page_id, true).unwrap();
                page_ids.push(page_id);
            }
            
            // Benchmark sequential access pattern
            b.iter(|| {
                for &page_id in &page_ids {
                    let page = buffer_pool.fetch_page(page_id).unwrap();
                    
                    // Read something from the page
                    {
                        let _page_guard = page.read();
                        // Just access the page to ensure it's loaded
                    }
                    
                    buffer_pool.unpin_page(page_id, false).unwrap();
                }
            });
        });
        
        group.bench_with_input(BenchmarkId::new("random_access", size), size, |b, &size| {
            let buffer_pool = create_test_buffer_pool(size as usize);
            let page_manager = PageManager::new();
            
            // Create some pages first
            let mut page_ids = Vec::new();
            for _ in 0..size {
                let (page, page_id) = buffer_pool.new_page().unwrap();
                
                // Initialize the page
                {
                    let mut page_guard = page.write();
                    page_manager.init_page(&mut page_guard);
                    
                    // Add some data
                    let data = generate_test_data(100);
                    page_manager.insert_record(&mut page_guard, &data).unwrap();
                }
                
                buffer_pool.unpin_page(page_id, true).unwrap();
                page_ids.push(page_id);
            }
            
            // Create random access pattern
            let mut rng = rand::thread_rng();
            let random_indices: Vec<usize> = (0..size as usize)
                .map(|_| rng.gen_range(0..size as usize))
                .collect();
            
            // Benchmark random access pattern
            b.iter(|| {
                for &idx in &random_indices {
                    let page_id = page_ids[idx];
                    let page = buffer_pool.fetch_page(page_id).unwrap();
                    
                    // Read something from the page
                    {
                        let _page_guard = page.read();
                        // Just access the page to ensure it's loaded
                    }
                    
                    buffer_pool.unpin_page(page_id, false).unwrap();
                }
            });
        });
    }
    
    group.finish();
}

criterion_group!(benches, buffer_pool_benchmark);
criterion_main!(benches); 