use std::sync::Arc;
use bayundb::common::types::{Rid, Page};
use bayundb::storage::buffer::manager::BufferPoolManager;
use bayundb::transaction::wal::log_manager::{LogManager, LogManagerConfig};
use bayundb::transaction::wal::log_buffer::LogBufferConfig;
use bayundb::transaction::{IsolationLevel, Transaction};
use bayundb::transaction::concurrency::transaction_manager::TransactionManager;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create log manager
    println!("Creating log manager...");
    let config = LogManagerConfig {
        log_dir: std::path::PathBuf::from("logs"),
        log_file_base_name: "transaction_demo".to_string(),
        max_log_file_size: 1024 * 1024, // 1 MB
        buffer_config: LogBufferConfig::default(),
        force_sync: true,
    };
    
    // Create directory if it doesn't exist
    std::fs::create_dir_all(&config.log_dir)?;
    
    let log_manager = Arc::new(LogManager::new(config)?);
    
    // Create buffer pool manager with WAL support
    println!("Creating buffer pool manager with WAL support...");
    let buffer_pool = BufferPoolManager::new_with_wal(
        10, // pool size
        "transaction_demo.db",
        log_manager.clone(),
    )?;
    
    // Create transaction manager
    println!("Creating transaction manager...");
    let txn_manager = TransactionManager::new(log_manager.clone());
    
    // Begin transaction 1
    println!("\n--- Starting Transaction 1 ---");
    let txn1_id = txn_manager.begin_transaction(IsolationLevel::ReadCommitted)?;
    println!("Started transaction with ID {}", txn1_id);
    
    // Get transaction object
    let txn1 = txn_manager.get_transaction(txn1_id).unwrap();
    
    // Create a new page using transaction
    println!("Creating a new page...");
    let (page1, page_id1) = buffer_pool.new_page_with_txn(&txn1)?;
    println!("Created new page with ID: {}", page_id1);
    
    // Insert a record
    println!("Inserting record...");
    let record_data = b"Transaction Demo Record 1";
    let rid = insert_record(page1.clone(), &txn1, page_id1, &buffer_pool, record_data)?;
    println!("Inserted record with RID: {}", rid);
    
    // Unpin the page
    buffer_pool.unpin_page(page_id1, true)?;
    
    // Begin transaction 2
    println!("\n--- Starting Transaction 2 ---");
    let txn2_id = txn_manager.begin_transaction(IsolationLevel::ReadCommitted)?;
    println!("Started transaction with ID {}", txn2_id);
    
    // Get transaction object
    let txn2 = txn_manager.get_transaction(txn2_id).unwrap();
    
    // Create a new page using transaction 2
    println!("Creating a new page in transaction 2...");
    let (page2, page_id2) = buffer_pool.new_page_with_txn(&txn2)?;
    println!("Created new page with ID: {}", page_id2);
    
    // Insert a record in transaction 2
    println!("Inserting record in transaction 2...");
    let record_data2 = b"Transaction Demo Record 2";
    let rid2 = insert_record(page2.clone(), &txn2, page_id2, &buffer_pool, record_data2)?;
    println!("Inserted record with RID: {}", rid2);
    
    // Update record in transaction 1
    println!("\nUpdating record in transaction 1...");
    let updated_data = b"Updated Transaction Record 1";
    update_record(page1.clone(), &txn1, page_id1, rid, &buffer_pool, record_data, updated_data)?;
    println!("Updated record with RID: {}", rid);
    
    // Commit transaction 1
    println!("\nCommitting transaction 1...");
    txn_manager.commit_transaction(txn1_id)?;
    println!("Transaction 1 committed");
    
    // Abort transaction 2
    println!("\nAborting transaction 2...");
    txn_manager.abort_transaction(txn2_id)?;
    println!("Transaction 2 aborted");
    
    // Flush all pages to disk
    println!("\nFlushing all pages to disk...");
    buffer_pool.flush_all_pages()?;
    
    println!("\nAll operations completed successfully!");
    println!("Log files are in the 'logs' directory.");
    println!("Database file is 'transaction_demo.db'");
    
    Ok(())
}

// Insert a record with the correct WAL logging
fn insert_record(
    page: Arc<parking_lot::RwLock<Page>>,
    txn: &Transaction,
    page_id: u32,
    buffer_pool: &BufferPoolManager,
    data: &[u8],
) -> Result<Rid, Box<dyn std::error::Error>> {
    let disk_manager = buffer_pool.disk_manager();
    let page_manager = disk_manager.page_manager();
    
    // Insert the record (without any WAL yet)
    let mut page_guard = page.write();
    let rid = page_manager.insert_record(&mut page_guard, data)?;
    
    // Log the insert operation
    let lsn = txn.log_insert(0, page_id, rid, data)?;
    
    // Update the page LSN
    page_guard.lsn = lsn;
    
    Ok(rid)
}

// Update a record with the correct WAL logging
fn update_record(
    page: Arc<parking_lot::RwLock<Page>>,
    txn: &Transaction,
    page_id: u32,
    rid: Rid,
    buffer_pool: &BufferPoolManager,
    old_data: &[u8],
    new_data: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let disk_manager = buffer_pool.disk_manager();
    let page_manager = disk_manager.page_manager();
    
    // Update the record (without any WAL yet)
    let mut page_guard = page.write();
    page_manager.update_record(&mut page_guard, rid, new_data)?;
    
    // Log the update operation
    let lsn = txn.log_update(0, page_id, rid, old_data, new_data)?;
    
    // Update the page LSN
    page_guard.lsn = lsn;
    
    Ok(())
} 