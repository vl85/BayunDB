use std::sync::Arc;
use std::path::PathBuf;
use std::io::Write;

use bayundb::storage::buffer::BufferPoolManager;
use bayundb::storage::page::PageManager;
use bayundb::index::btree::BTreeIndex;
use bayundb::transaction::wal::log_manager::{LogManager, LogManagerConfig};
use bayundb::transaction::wal::log_buffer::LogBufferConfig;
use bayundb::transaction::recovery::TransactionRecoveryManager;
use bayundb::transaction::concurrency::transaction_manager::TransactionManager;
use bayundb::transaction::IsolationLevel;

// Main function with proper error handling
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Recovery Debug Example ===");
    println!("This example simulates a recovery test scenario with controlled file access");
    
    // Step 1: Set up directories with proper permissions
    println!("\nStep 1: Setting up directories with explicit permissions checks");
    
    // Create database directory with explicit permission checks
    let db_dir = std::env::temp_dir().join("bayundb_recovery_test_db");
    std::fs::create_dir_all(&db_dir)?;
    println!("Created DB directory: {:?}", db_dir);
    
    // Verify write permissions with a test file
    let test_file = db_dir.join("test_write.txt");
    let mut file = std::fs::File::create(&test_file)?;
    file.write_all(b"Test write permissions")?;
    println!("Successfully wrote to test file: {:?}", test_file);
    std::fs::remove_file(&test_file)?;
    println!("Removed test file successfully");
    
    // Create log directory with explicit permission checks
    let log_dir = std::env::temp_dir().join("bayundb_recovery_test_logs");
    std::fs::create_dir_all(&log_dir)?;
    println!("Created log directory: {:?}", log_dir);
    
    // Verify write permissions with a test file
    let test_log_file = log_dir.join("test_write.txt");
    let mut log_file = std::fs::File::create(&test_log_file)?;
    log_file.write_all(b"Test write permissions")?;
    println!("Successfully wrote to test log file: {:?}", test_log_file);
    std::fs::remove_file(&test_log_file)?;
    println!("Removed test log file successfully");
    
    // Step 2: Create log manager with explicit configuration
    println!("\nStep 2: Creating log manager");
    let log_config = LogManagerConfig {
        log_dir: log_dir.clone(),
        log_file_base_name: "recovery_debug".to_string(),
        max_log_file_size: 1024 * 1024, // 1 MB
        buffer_config: LogBufferConfig::default(),
        force_sync: true, // Force sync for recovery testing
    };
    
    let log_manager = Arc::new(LogManager::new(log_config)?);
    println!("Log manager created successfully");
    
    // Step 3: Create buffer pool manager with WAL support
    println!("\nStep 3: Creating buffer pool manager with WAL");
    let db_file = db_dir.join("recovery_debug.db");
    let buffer_pool = Arc::new(BufferPoolManager::new_with_wal(
        100, // Buffer pool size
        db_file.as_path(),
        log_manager.clone(),
    )?);
    println!("Buffer pool manager created successfully");
    
    // Step 4: Create transaction manager
    println!("\nStep 4: Creating transaction manager");
    let txn_manager = Arc::new(TransactionManager::new(log_manager.clone()));
    println!("Transaction manager created successfully");
    
    // Step 5: Create page manager
    let page_manager = PageManager::new();
    
    // Step 6: Create B+Tree index
    println!("\nStep 6: Creating B+Tree index");
    let btree = BTreeIndex::<i32>::new(buffer_pool.clone())?;
    println!("B+Tree index created successfully");
    
    // Step 7: Create and insert records with transaction
    println!("\nStep 7: Creating and inserting records with transaction");
    
    // Begin a transaction
    let txn_id = txn_manager.begin_transaction(IsolationLevel::ReadCommitted)?;
    println!("Started transaction with ID {}", txn_id);
    let txn = txn_manager.get_transaction(txn_id).unwrap();
    
    // Store record info for verification
    let mut record_info = Vec::new();
    
    // Test data
    let records = [
        (1, b"Record for key 1".to_vec()),
        (5, b"Record for key 5".to_vec()),
        (10, b"Record for key 10".to_vec()),
    ];
    
    // Insert records with transaction
    for &(key, ref data) in &records {
        // Create a new page for the record
        let (page, page_id) = buffer_pool.new_page_with_txn(&txn)?;
        
        // Initialize page and insert record
        let rid = {
            let mut page_guard = page.write();
            page_manager.init_page(&mut page_guard);
            
            // Insert record
            let rid = page_manager.insert_record(&mut page_guard, data)?;
            
            // Log the insert operation
            txn.log_insert(0, page_id, rid, data)?;
            
            // Add to index
            btree.insert(key, rid)?;
            
            rid
        };
        
        // Store record info for verification
        record_info.push((key, page_id, rid));
        
        // Unpin the page
        buffer_pool.unpin_page_with_txn(page_id, true, Some(&txn), None)?;
        
        println!("Inserted record: key={}, page_id={}, rid={}", key, page_id, rid);
    }
    
    // Commit the transaction
    txn_manager.commit_transaction(txn_id)?;
    println!("Committed transaction {}", txn_id);
    
    // Create a checkpoint
    log_manager.checkpoint(
        &txn_manager.get_active_transaction_ids(), 
        &[] // No dirty pages since we committed
    )?;
    println!("Created checkpoint");
    
    // Step 8: Start a second transaction that will be "interrupted"
    println!("\nStep 8: Starting a second transaction (will not be committed)");
    let txn_id2 = txn_manager.begin_transaction(IsolationLevel::ReadCommitted)?;
    println!("Started transaction with ID {}", txn_id2);
    let txn2 = txn_manager.get_transaction(txn_id2).unwrap();
    
    // Update a record
    let (key_to_update, page_id_to_update, rid_to_update) = record_info[1]; // Update the second record
    let updated_data = b"UPDATED: This should be rolled back".to_vec();
    
    // Fetch the page and update the record
    {
        let page = buffer_pool.fetch_page(page_id_to_update)?;
        
        // Get the original data for before-image
        let original_data = {
            let page_guard = page.read();
            page_manager.get_record(&page_guard, rid_to_update)?.to_vec()
        };
        
        // Update the record
        {
            let mut page_guard = page.write();
            page_manager.update_record(&mut page_guard, rid_to_update, &updated_data)?;
            
            // Log the update operation
            txn2.log_update(0, page_id_to_update, rid_to_update, &original_data, &updated_data)?;
        }
        
        // Unpin the page
        buffer_pool.unpin_page_with_txn(page_id_to_update, true, Some(&txn2), None)?;
    }
    
    // Flush buffer pool to ensure changes are on disk
    buffer_pool.flush_all_pages()?;
    println!("Updated record, key={} with new data: {:?}", key_to_update, String::from_utf8_lossy(&updated_data));
    
    // Step 9: Simulate a crash (don't commit the second transaction)
    println!("\nStep 9: Simulating crash (transaction {} not committed)", txn_id2);
    
    // Step 10: Perform recovery
    println!("\nStep 10: Starting recovery process");
    
    // Create a new buffer pool manager that points to the same database file
    println!("Creating new buffer pool manager for recovery");
    let buffer_pool_after_crash = Arc::new(BufferPoolManager::new_with_wal(
        100,
        db_file.as_path(),
        log_manager.clone(),
    )?);
    println!("New buffer pool manager created successfully");
    
    // Create recovery manager
    println!("Creating recovery manager");
    let mut recovery_manager = TransactionRecoveryManager::new(
        log_manager.clone(),
        buffer_pool_after_crash.clone()
    );
    println!("Recovery manager created successfully");
    
    // Run recovery process
    println!("Running recovery process");
    recovery_manager.recover()?;
    println!("Recovery process completed");
    
    // Print recovery statistics
    recovery_manager.print_recovery_statistics();
    
    // Step 11: Verify data after recovery
    println!("\nStep 11: Verifying data after recovery");
    
    // Create a new B+Tree to verify index
    let btree_after_recovery = BTreeIndex::<i32>::new(buffer_pool_after_crash.clone())?;
    
    for (i, &(key, page_id, rid)) in record_info.iter().enumerate() {
        // Verify index entries exist
        let rids = btree_after_recovery.find(&key)?;
        println!("Index for key {} has {} entries", key, rids.len());
        
        // Fetch and verify page data
        let page = buffer_pool_after_crash.fetch_page(page_id)?;
        let data = {
            let page_guard = page.read();
            page_manager.get_record(&page_guard, rid)?
        };
        
        println!("Record {}: key={}, data={:?}", i, key, String::from_utf8_lossy(&data));
        
        // If this is the record that was updated in the uncommitted transaction,
        // verify it has the original value, not the updated one
        if key == key_to_update {
            if data == updated_data {
                println!("ERROR: Record for key {} has uncommitted update!", key);
            } else {
                println!("SUCCESS: Record for key {} was properly rolled back", key);
            }
        }
        
        buffer_pool_after_crash.unpin_page(page_id, false)?;
    }
    
    println!("\nRecovery debug example completed successfully");
    
    // Clean up
    drop(buffer_pool);
    drop(buffer_pool_after_crash);
    drop(log_manager);
    
    // Wait a moment to ensure all files are closed
    std::thread::sleep(std::time::Duration::from_millis(500));
    
    // Optionally remove the test directories
    // Comment these out if you want to examine the files after running
    // std::fs::remove_dir_all(&db_dir)?;
    // std::fs::remove_dir_all(&log_dir)?;
    
    Ok(())
} 