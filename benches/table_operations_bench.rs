use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use rand::prelude::*;
use std::sync::Arc;
use std::time::Duration;

use bayundb::storage::buffer::BufferPoolManager;
use bayundb::storage::page::PageManager;

// Create temporary database environment
fn setup_test_environment(buffer_pool_size: usize) -> Arc<BufferPoolManager> {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    let buffer_pool = Arc::new(BufferPoolManager::new(buffer_pool_size, path).unwrap());
    
    // Keep the temp file alive
    std::mem::forget(temp_file);
    
    buffer_pool
}

// Generate test record data
fn generate_test_record(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.gen::<u8>()).collect()
}

fn table_operations_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("TableOperations");
    
    // Configure benchmarks
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);
    
    // Test with different record counts
    for &record_count in &[100, 1000, 10000] {
        // Benchmark record insertion
        group.bench_with_input(BenchmarkId::new("insert_records", record_count), &record_count, |b, &record_count| {
            let buffer_pool = setup_test_environment(1000);
            let page_manager = PageManager::new();
            
            // Create a page to simulate a table
            let (page, page_id) = buffer_pool.new_page().unwrap();
            
            {
                let mut page_guard = page.write();
                page_manager.init_page(&mut page_guard);
            }
            
            buffer_pool.unpin_page(page_id, true).unwrap();
            
            // Prepare test records
            let records: Vec<Vec<u8>> = (0..record_count)
                .map(|_| generate_test_record(100))
                .collect();
            let mut idx = 0;
            
            b.iter(|| {
                if idx >= records.len() {
                    idx = 0;
                }
                
                let page = buffer_pool.fetch_page(page_id).unwrap();
                let record = &records[idx];
                
                {
                    let mut page_guard = page.write();
                    // Insert a record
                    let _ = page_manager.insert_record(&mut page_guard, record).unwrap();
                }
                
                buffer_pool.unpin_page(page_id, true).unwrap();
                idx += 1;
            });
        });
        
        // Benchmark full table scan
        group.bench_with_input(BenchmarkId::new("table_scan", record_count), &record_count, |b, &record_count| {
            let buffer_pool = setup_test_environment(1000);
            let page_manager = PageManager::new();
            
            // Create a page and populate with records
            let (page, page_id) = buffer_pool.new_page().unwrap();
            
            {
                let mut page_guard = page.write();
                page_manager.init_page(&mut page_guard);
                
                // Insert test records
                for _ in 0..record_count {
                    let record = generate_test_record(100);
                    page_manager.insert_record(&mut page_guard, &record).unwrap();
                }
            }
            
            buffer_pool.unpin_page(page_id, true).unwrap();
            
            // Benchmark scanning all records
            b.iter(|| {
                let page = buffer_pool.fetch_page(page_id).unwrap();
                
                {
                    let page_guard = page.read();
                    let record_count = page_manager.get_record_count(&page_guard);
                    
                    // Scan all records
                    for slot_id in 0..record_count {
                        if page_manager.is_slot_occupied(&page_guard, slot_id) {
                            let _record = page_manager.get_record(&page_guard, slot_id).unwrap();
                            // Simply read the record, don't do anything with it
                        }
                    }
                }
                
                buffer_pool.unpin_page(page_id, false).unwrap();
            });
        });
        
        // Benchmark record lookup by slot ID
        group.bench_with_input(BenchmarkId::new("record_lookup", record_count), &record_count, |b, &record_count| {
            let buffer_pool = setup_test_environment(1000);
            let page_manager = PageManager::new();
            
            // Create a page and populate with records
            let (page, page_id) = buffer_pool.new_page().unwrap();
            
            {
                let mut page_guard = page.write();
                page_manager.init_page(&mut page_guard);
                
                // Insert test records
                for _ in 0..record_count {
                    let record = generate_test_record(100);
                    page_manager.insert_record(&mut page_guard, &record).unwrap();
                }
            }
            
            buffer_pool.unpin_page(page_id, true).unwrap();
            
            // Prepare random slot IDs to lookup
            let mut rng = rand::thread_rng();
            let slots: Vec<u16> = (0..record_count as u16).collect();
            let mut idx = 0;
            
            // Benchmark looking up random records
            b.iter(|| {
                if idx >= slots.len() {
                    idx = 0;
                }
                
                let slot_id = slots[idx];
                let page = buffer_pool.fetch_page(page_id).unwrap();
                
                {
                    let page_guard = page.read();
                    if page_manager.is_slot_occupied(&page_guard, slot_id) {
                        let _record = page_manager.get_record(&page_guard, slot_id).unwrap();
                        // Simply read the record
                    }
                }
                
                buffer_pool.unpin_page(page_id, false).unwrap();
                idx += 1;
            });
        });
        
        // Benchmark record updates
        group.bench_with_input(BenchmarkId::new("record_update", record_count), &record_count, |b, &record_count| {
            let buffer_pool = setup_test_environment(1000);
            let page_manager = PageManager::new();
            
            // Create a page and populate with records
            let (page, page_id) = buffer_pool.new_page().unwrap();
            let mut slot_ids = Vec::with_capacity(record_count);
            
            {
                let mut page_guard = page.write();
                page_manager.init_page(&mut page_guard);
                
                // Insert test records and save their slot IDs
                for _ in 0..record_count {
                    let record = generate_test_record(100);
                    let slot_id = page_manager.insert_record(&mut page_guard, &record).unwrap();
                    slot_ids.push(slot_id);
                }
            }
            
            buffer_pool.unpin_page(page_id, true).unwrap();
            
            // Prepare new records for updates
            let update_records: Vec<Vec<u8>> = (0..record_count)
                .map(|_| generate_test_record(100))
                .collect();
            let mut idx = 0;
            
            // Benchmark updating records
            b.iter(|| {
                if idx >= slot_ids.len() {
                    idx = 0;
                }
                
                let slot_id = slot_ids[idx];
                let update_record = &update_records[idx];
                let page = buffer_pool.fetch_page(page_id).unwrap();
                
                {
                    let mut page_guard = page.write();
                    // Update the record
                    page_manager.update_record(&mut page_guard, slot_id, update_record).unwrap();
                }
                
                buffer_pool.unpin_page(page_id, true).unwrap();
                idx += 1;
            });
        });
    }
    
    group.finish();
}

criterion_group!(benches, table_operations_benchmark);
criterion_main!(benches); 