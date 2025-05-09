use std::sync::Arc;
use bayundb::transaction::wal::log_manager::{LogManager, LogManagerConfig};
use bayundb::transaction::wal::log_record::{LogRecordType, LogRecordContent, TransactionOperationContent, DataOperationContent};
use bayundb::transaction::wal::log_buffer::LogBufferConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a log manager with default configuration
    println!("Creating log manager...");
    let config = LogManagerConfig {
        log_dir: std::path::PathBuf::from("logs"),
        log_file_base_name: "wal_demo".to_string(),
        max_log_file_size: 1024 * 1024, // 1 MB
        buffer_config: LogBufferConfig::default(),
        force_sync: true,
    };
    
    let log_manager = Arc::new(LogManager::new(config)?);
    
    println!("Log manager created. Starting transaction simulation...");
    
    // Simulate a transaction
    let txn_id = 1;
    let mut prev_lsn = 0;
    
    // BEGIN transaction
    println!("BEGIN transaction {}", txn_id);
    prev_lsn = log_manager.append_log_record(
        txn_id,
        prev_lsn,
        LogRecordType::Begin,
        LogRecordContent::Transaction(TransactionOperationContent {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            metadata: None,
        }),
    )?;
    
    // INSERT operation
    println!("INSERT record into table 1, page 1");
    prev_lsn = log_manager.append_log_record(
        txn_id,
        prev_lsn,
        LogRecordType::Insert,
        LogRecordContent::Data(DataOperationContent {
            table_id: 1,
            page_id: 1,
            record_id: 100,
            before_image: None,
            after_image: Some(b"Hello, WAL!".to_vec()),
        }),
    )?;
    
    // UPDATE operation
    println!("UPDATE record in table 1, page 1");
    prev_lsn = log_manager.append_log_record(
        txn_id,
        prev_lsn,
        LogRecordType::Update,
        LogRecordContent::Data(DataOperationContent {
            table_id: 1,
            page_id: 1,
            record_id: 100,
            before_image: Some(b"Hello, WAL!".to_vec()),
            after_image: Some(b"Hello, WAL Updated!".to_vec()),
        }),
    )?;
    
    // COMMIT transaction
    println!("COMMIT transaction {}", txn_id);
    prev_lsn = log_manager.append_log_record(
        txn_id,
        prev_lsn,
        LogRecordType::Commit,
        LogRecordContent::Transaction(TransactionOperationContent {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            metadata: None,
        }),
    )?;
    
    // Simulate a second transaction that aborts
    let txn_id = 2;
    let mut prev_lsn = 0;
    
    // BEGIN transaction
    println!("\nBEGIN transaction {}", txn_id);
    prev_lsn = log_manager.append_log_record(
        txn_id,
        prev_lsn,
        LogRecordType::Begin,
        LogRecordContent::Transaction(TransactionOperationContent {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            metadata: None,
        }),
    )?;
    
    // DELETE operation
    println!("DELETE record from table 2, page 1");
    prev_lsn = log_manager.append_log_record(
        txn_id,
        prev_lsn,
        LogRecordType::Delete,
        LogRecordContent::Data(DataOperationContent {
            table_id: 2,
            page_id: 1,
            record_id: 200,
            before_image: Some(b"Record to delete".to_vec()),
            after_image: None,
        }),
    )?;
    
    // ABORT transaction
    println!("ABORT transaction {}", txn_id);
    prev_lsn = log_manager.append_log_record(
        txn_id,
        prev_lsn,
        LogRecordType::Abort,
        LogRecordContent::Transaction(TransactionOperationContent {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            metadata: None,
        }),
    )?;
    
    // Create a checkpoint
    println!("\nCreating checkpoint...");
    let checkpoint_lsn = log_manager.checkpoint(&[3], &[(1, 10), (2, 20)])?;
    println!("Checkpoint created with LSN: {}", checkpoint_lsn);
    
    // Final flush to make sure all data is on disk
    log_manager.flush()?;
    
    println!("\nAll operations completed successfully!");
    println!("Log files are in the 'logs' directory.");
    
    Ok(())
} 