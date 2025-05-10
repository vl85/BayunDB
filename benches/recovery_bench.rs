use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use rand::Rng;

use bayundb::storage::buffer::BufferPoolManager;
use bayundb::storage::page::PageManager;
use bayundb::transaction::wal::log_manager::{LogManager, LogManagerConfig};
use bayundb::transaction::wal::log_buffer::LogBufferConfig;
use bayundb::transaction::recovery::TransactionRecoveryManager;
use bayundb::transaction::concurrency::transaction_manager::TransactionManager;
use bayundb::transaction::IsolationLevel;

// Helper to create a test environment with configurable parameters
fn setup_test_environment(
    buffer_pool_size: usize,
    log_buffer_size: usize,
    force_sync: bool,
) -> (
    Arc<LogManager>,
    Arc<BufferPoolManager>,
    Arc<TransactionManager>,
    PageManager,
    TempDir,
    TempDir,
) {
    // Create database directory
    let db_dir = tempfile::tempdir().unwrap();
    let db_path = db_dir.path().to_str().unwrap().to_string();
    
    // Create log directory
    let log_dir = tempfile::tempdir().unwrap();
    let log_path = log_dir.path().to_str().unwrap().to_string();
    
    // Configure log manager
    let log_config = LogManagerConfig {
        log_dir: std::path::PathBuf::from(&log_path),
        log_file_base_name: "bench_log".to_string(),
        max_log_file_size: 1024 * 1024, // 1 MB
        buffer_config: LogBufferConfig {
            buffer_size: log_buffer_size,
            flush_threshold: 0.75,
        },
        force_sync,
    };
    
    let log_manager = Arc::new(LogManager::new(log_config).unwrap());
    
    // Create database file path
    let db_file = std::path::Path::new(&db_path).join("bench_db.db");
    
    // Create buffer pool with WAL support
    let buffer_pool = Arc::new(BufferPoolManager::new_with_wal(
        buffer_pool_size,
        &db_file,
        log_manager.clone(),
    ).unwrap());
    
    // Create transaction manager
    let txn_manager = Arc::new(TransactionManager::new(log_manager.clone()));
    
    // Create page manager
    let page_manager = PageManager::new();
    
    (log_manager, buffer_pool, txn_manager, page_manager, db_dir, log_dir)
}

// Generate transaction workload with the specified number of operations
fn generate_workload(
    op_count: usize,
    buffer_pool: &Arc<BufferPoolManager>,
    txn_manager: &Arc<TransactionManager>,
    page_manager: &PageManager,
) -> Vec<u32> {
    let mut page_ids = Vec::new();
    let mut rng = rand::thread_rng();
    
    // Begin transaction
    let txn_id = txn_manager.begin_transaction(IsolationLevel::ReadCommitted).unwrap();
    let txn = txn_manager.get_transaction(txn_id).unwrap();
    
    for _ in 0..op_count {
        // Create a new page
        let (page, page_id) = buffer_pool.new_page_with_txn(&txn).unwrap();
        
        // Initialize page and insert data
        let record_data = (0..100).map(|_| rng.gen_range(0..=255) as u8).collect::<Vec<_>>();
        {
            let mut page_guard = page.write();
            page_manager.init_page(&mut page_guard);
            
            // Insert a record
            let rid = page_manager.insert_record(&mut page_guard, &record_data).unwrap();
            
            // Log the insert
            txn.log_insert(0, page_id, rid.slot_num, &record_data).unwrap();
        }
        
        // Unpin page
        buffer_pool.unpin_page_with_txn(page_id, true, Some(&txn), None).unwrap();
        
        page_ids.push(page_id);
    }
    
    // Commit the transaction
    txn_manager.commit_transaction(txn_id).unwrap();
    
    page_ids
}

// Benchmark WAL append performance
fn bench_wal_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("WAL_Append");
    group.measurement_time(Duration::from_secs(5));
    
    // Test with different transaction sizes
    for op_count in [10, 100, 1000].iter() {
        // Test with both sync and async WAL
        for force_sync in [false, true].iter() {
            let config_name = format!("ops_{}_sync_{}", op_count, force_sync);
            
            group.bench_with_input(BenchmarkId::new("wal_append", &config_name), &op_count, |b, &op_count| {
                let (_log_manager, buffer_pool, txn_manager, page_manager, _db_dir, _log_dir) = 
                    setup_test_environment(1000, 1024 * 1024, *force_sync);
                
                b.iter(|| {
                    generate_workload(*op_count, &buffer_pool, &txn_manager, &page_manager);
                });
            });
        }
    }
    
    group.finish();
}

// Benchmark recovery time after simulated crash
fn bench_recovery_time(c: &mut Criterion) {
    let mut group = c.benchmark_group("Recovery_Time");
    group.measurement_time(Duration::from_secs(5));
    
    // Test with different workload sizes
    for op_count in [10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::new("recovery", op_count), &op_count, |b, &op_count| {
            let (log_manager, buffer_pool, txn_manager, page_manager, db_dir, _log_dir) = 
                setup_test_environment(1000, 1024 * 1024, true);
            
            // Create workload with committed transactions
            generate_workload(*op_count, &buffer_pool, &txn_manager, &page_manager);
            
            // Create an uncommitted transaction
            let txn_id = txn_manager.begin_transaction(IsolationLevel::ReadCommitted).unwrap();
            let txn = txn_manager.get_transaction(txn_id).unwrap();
            
            // Add some uncommitted operations
            let mut rng = rand::thread_rng();
            for _ in 0..(*op_count / 5) {
                let (page, page_id) = buffer_pool.new_page_with_txn(&txn).unwrap();
                
                let record_data = (0..100).map(|_| rng.gen_range(0..=255) as u8).collect::<Vec<_>>();
                {
                    let mut page_guard = page.write();
                    page_manager.init_page(&mut page_guard);
                    
                    let rid = page_manager.insert_record(&mut page_guard, &record_data).unwrap();
                    txn.log_insert(0, page_id, rid.slot_num, &record_data).unwrap();
                }
                
                buffer_pool.unpin_page_with_txn(page_id, true, Some(&txn), None).unwrap();
            }
            
            // Flush buffer pool to ensure changes are on disk
            buffer_pool.flush_all_pages().unwrap();
            
            // Get the database file path
            let db_path = std::path::Path::new(db_dir.path()).join("bench_db.db");
            
            // Benchmark recovery time
            b.iter(|| {
                // Create a new buffer pool manager that points to the same database file
                let buffer_pool_after_crash = Arc::new(BufferPoolManager::new_with_wal(
                    1000,
                    &db_path,
                    log_manager.clone(),
                ).unwrap());
                
                // Create recovery manager
                let mut recovery_manager = TransactionRecoveryManager::new(
                    log_manager.clone(),
                    buffer_pool_after_crash.clone()
                );
                
                // Run recovery process
                recovery_manager.recover().unwrap();
            });
        });
    }
    
    group.finish();
}

// Benchmark checkpoint performance
fn bench_checkpoint(c: &mut Criterion) {
    let mut group = c.benchmark_group("Checkpoint");
    group.measurement_time(Duration::from_secs(5));
    
    // Test with different log sizes
    for op_count in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::new("checkpoint", op_count), &op_count, |b, &op_count| {
            let (log_manager, buffer_pool, txn_manager, page_manager, _db_dir, _log_dir) = 
                setup_test_environment(1000, 1024 * 1024, false);
            
            // Generate workload
            generate_workload(*op_count, &buffer_pool, &txn_manager, &page_manager);
            
            // Benchmark checkpoint operation
            b.iter(|| {
                log_manager.checkpoint(
                    &[], // No active transactions 
                    &[] // No dirty pages
                ).unwrap();
            });
        });
    }
    
    group.finish();
}

// Benchmark recovery with different checkpoint frequencies
fn bench_recovery_with_checkpoints(c: &mut Criterion) {
    let mut group = c.benchmark_group("Recovery_With_Checkpoints");
    group.measurement_time(Duration::from_secs(5));
    
    // Test with different checkpoint frequencies
    for checkpoint_interval in [10, 100, 500].iter() {
        group.bench_with_input(BenchmarkId::new("recovery_checkpoint", checkpoint_interval), &checkpoint_interval, |b, &checkpoint_interval| {
            let (log_manager, buffer_pool, txn_manager, page_manager, db_dir, _log_dir) = 
                setup_test_environment(1000, 1024 * 1024, false);
            
            // Total operations to perform
            let total_ops = 1000;
            
            // Perform operations with checkpoints
            let mut ops_since_checkpoint = 0;
            for _ in 0..total_ops {
                // Begin transaction
                let txn_id = txn_manager.begin_transaction(IsolationLevel::ReadCommitted).unwrap();
                let txn = txn_manager.get_transaction(txn_id).unwrap();
                
                // Create a new page and add data
                let (page, page_id) = buffer_pool.new_page_with_txn(&txn).unwrap();
                let mut rng = rand::thread_rng();
                let record_data = (0..100).map(|_| rng.gen_range(0..=255) as u8).collect::<Vec<_>>();
                
                {
                    let mut page_guard = page.write();
                    page_manager.init_page(&mut page_guard);
                    
                    let rid = page_manager.insert_record(&mut page_guard, &record_data).unwrap();
                    txn.log_insert(0, page_id, rid.slot_num, &record_data).unwrap();
                }
                
                buffer_pool.unpin_page_with_txn(page_id, true, Some(&txn), None).unwrap();
                
                // Commit the transaction
                txn_manager.commit_transaction(txn_id).unwrap();
                
                // Increment operations counter
                ops_since_checkpoint += 1;
                
                // Create checkpoint if needed
                if ops_since_checkpoint >= *checkpoint_interval {
                    log_manager.checkpoint(
                        &[], // No active transactions 
                        &[] // No dirty pages
                    ).unwrap();
                    ops_since_checkpoint = 0;
                }
            }
            
            // Flush buffer pool to ensure changes are on disk
            buffer_pool.flush_all_pages().unwrap();
            
            // Get the database file path
            let db_path = std::path::Path::new(db_dir.path()).join("bench_db.db");
            
            // Benchmark recovery time
            b.iter(|| {
                // Create a new buffer pool manager that points to the same database file
                let buffer_pool_after_crash = Arc::new(BufferPoolManager::new_with_wal(
                    1000,
                    &db_path,
                    log_manager.clone(),
                ).unwrap());
                
                // Create recovery manager
                let mut recovery_manager = TransactionRecoveryManager::new(
                    log_manager.clone(),
                    buffer_pool_after_crash.clone()
                );
                
                // Run recovery process
                recovery_manager.recover().unwrap();
            });
        });
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_wal_append,
    bench_recovery_time,
    bench_checkpoint,
    bench_recovery_with_checkpoints
);
criterion_main!(benches); 