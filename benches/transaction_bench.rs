use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::NamedTempFile;

use bayundb::common::types::{PageId, Rid};
use bayundb::storage::buffer::BufferPoolManager;
use bayundb::storage::page::PageManager;
use bayundb::transaction::{TransactionManager, IsolationLevel};
use bayundb::transaction::wal::log_manager::{LogManager, LogManagerConfig};
use bayundb::transaction::wal::log_buffer::LogBufferConfig;

// Create a test environment for transaction benchmarks
fn setup_test_environment(buffer_pool_size: usize) -> (Arc<BufferPoolManager>, Arc<LogManager>, Arc<TransactionManager>) {
    // Create temporary directory for logs
    let log_dir = tempfile::tempdir().unwrap();
    let log_path = log_dir.path().to_str().unwrap().to_string();
    
    // Create log manager with temporary directory
    let log_config = LogManagerConfig {
        log_dir: std::path::PathBuf::from(&log_path),
        log_file_base_name: "bench_log".to_string(),
        max_log_file_size: 1024 * 1024, // 1 MB
        buffer_config: LogBufferConfig::default(),
        force_sync: false, // For benchmarks, don't force sync on every operation
    };
    
    let log_manager = Arc::new(LogManager::new(log_config).unwrap());
    
    // Create a temporary file for the database
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path().to_str().unwrap().to_string();
    
    // Create buffer pool with WAL support
    let buffer_pool = Arc::new(BufferPoolManager::new_with_wal(
        buffer_pool_size,
        db_path,
        log_manager.clone(),
    ).unwrap());
    
    // Create transaction manager
    let txn_manager = Arc::new(TransactionManager::new(log_manager.clone()));
    
    // Keep the temp file and directory alive
    std::mem::forget(temp_file);
    std::mem::forget(log_dir);
    
    (buffer_pool, log_manager, txn_manager)
}

// Helper to create a page and insert a record using WAL
fn insert_test_record(buffer_pool: &Arc<BufferPoolManager>, txn_id: u32, txn_manager: &Arc<TransactionManager>) -> (PageId, Rid) {
    let txn = txn_manager.get_transaction(txn_id).unwrap();
    
    // Create a new page
    let (page, page_id) = buffer_pool.new_page_with_txn(&txn).unwrap();
    
    // Insert a record
    let mut page_guard = page.write();
    let record_data = b"Benchmark test record";
    
    // Get the page manager and insert a record
    let page_manager = PageManager::new();
    
    let rid = page_manager.insert_record(&mut page_guard, record_data).unwrap();
    
    // Log the insertion
    let lsn = txn.log_insert(0, page_id, rid.slot_num, record_data).unwrap();
    
    // Update page LSN
    page_guard.lsn = lsn;
    
    // Unpin the page
    drop(page_guard);
    buffer_pool.unpin_page(page_id, true).unwrap();
    
    (page_id, rid)
}

fn transaction_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Transaction");
    
    // Configure benchmarks
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(30);
    
    // Benchmark single transaction performance
    group.bench_function("single_transaction", |b| {
        let (buffer_pool, _log_manager, txn_manager) = setup_test_environment(1000);
        
        b.iter(|| {
            // Begin transaction
            let txn_id = txn_manager.begin_transaction(IsolationLevel::ReadCommitted).unwrap();
            
            // Insert a record
            let (_page_id, _rid) = insert_test_record(&buffer_pool, txn_id, &txn_manager);
            
            // Commit transaction
            txn_manager.commit_transaction(txn_id).unwrap();
        });
    });
    
    // Benchmark concurrent transactions with different thread counts
    for &thread_count in &[2, 4, 8] {
        group.bench_with_input(BenchmarkId::new("concurrent_transactions", thread_count), &thread_count, |b, &thread_count| {
            let (buffer_pool, _log_manager, txn_manager) = setup_test_environment(1000);
            
            b.iter(|| {
                let mut handles = vec![];
                
                // Spawn multiple threads each running transactions
                for _i in 0..thread_count {
                    let buffer_pool_clone = buffer_pool.clone();
                    let txn_manager_clone = txn_manager.clone();
                    
                    let handle = thread::spawn(move || {
                        for _j in 0..5 {  // Each thread runs 5 transactions
                            let txn_id = txn_manager_clone.begin_transaction(IsolationLevel::ReadCommitted).unwrap();
                            
                            // Insert a record
                            let (_page_id, _rid) = insert_test_record(&buffer_pool_clone, txn_id, &txn_manager_clone);
                            
                            // Commit transaction
                            txn_manager_clone.commit_transaction(txn_id).unwrap();
                        }
                    });
                    
                    handles.push(handle);
                }
                
                // Wait for all threads to complete
                for handle in handles {
                    handle.join().unwrap();
                }
            });
        });
    }
    
    // Benchmark different isolation levels
    let isolation_levels = [
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
        IsolationLevel::RepeatableRead,
        IsolationLevel::Serializable
    ];
    
    for isolation_level in isolation_levels {
        let level_name = format!("{:?}", isolation_level);
        group.bench_with_input(BenchmarkId::new("isolation_level", level_name), &isolation_level, |b, &isolation_level| {
            let (buffer_pool, _log_manager, txn_manager) = setup_test_environment(1000);
            
            b.iter(|| {
                // Begin transaction with specific isolation level
                let txn_id = txn_manager.begin_transaction(isolation_level).unwrap();
                
                // Insert a record
                let (_page_id, _rid) = insert_test_record(&buffer_pool, txn_id, &txn_manager);
                
                // Commit transaction
                txn_manager.commit_transaction(txn_id).unwrap();
            });
        });
    }
    
    // Benchmark transaction abort/rollback
    group.bench_function("transaction_rollback", |b| {
        let (buffer_pool, _log_manager, txn_manager) = setup_test_environment(1000);
        
        b.iter(|| {
            // Begin transaction
            let txn_id = txn_manager.begin_transaction(IsolationLevel::ReadCommitted).unwrap();
            
            // Insert a record
            let (_page_id, _rid) = insert_test_record(&buffer_pool, txn_id, &txn_manager);
            
            // Abort/rollback the transaction
            txn_manager.abort_transaction(txn_id).unwrap();
        });
    });
    
    group.finish();
}

criterion_group!(benches, transaction_benchmark);
criterion_main!(benches); 