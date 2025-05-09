use std::sync::Arc;
use std::path::PathBuf;
use tempfile::TempDir;
use bayundb::transaction::wal::log_manager::{LogManager, LogManagerConfig};
use bayundb::transaction::wal::log_record::{LogRecordType, LogRecordContent, TransactionOperationContent, DataOperationContent};
use bayundb::transaction::wal::log_buffer::LogBufferConfig;

fn create_test_log_manager() -> (Arc<LogManager>, TempDir) {
    // Create a temporary directory for the log files
    let temp_dir = TempDir::new().unwrap();
    
    // Create a configuration
    let config = LogManagerConfig {
        log_dir: temp_dir.path().to_path_buf(),
        log_file_base_name: "test_log".to_string(),
        max_log_file_size: 1024 * 1024, // 1 MB
        buffer_config: LogBufferConfig::default(),
        force_sync: false, // No need to sync for tests
    };
    
    // Create the log manager
    let log_manager = Arc::new(LogManager::new(config).unwrap());
    
    (log_manager, temp_dir)
}

#[test]
fn test_wal_basic_operations() -> anyhow::Result<()> {
    // Create a log manager
    let (log_manager, _temp_dir) = create_test_log_manager();
    
    // Simulate a transaction
    let txn_id = 1;
    let mut prev_lsn = 0;
    
    // BEGIN transaction
    prev_lsn = log_manager.append_log_record(
        txn_id,
        prev_lsn,
        LogRecordType::Begin,
        LogRecordContent::Transaction(TransactionOperationContent {
            timestamp: 1000,
            metadata: None,
        }),
    )?;
    
    // INSERT operation
    prev_lsn = log_manager.append_log_record(
        txn_id,
        prev_lsn,
        LogRecordType::Insert,
        LogRecordContent::Data(DataOperationContent {
            table_id: 1,
            page_id: 1,
            record_id: 100,
            before_image: None,
            after_image: Some(b"Test record".to_vec()),
        }),
    )?;
    
    // COMMIT transaction
    prev_lsn = log_manager.append_log_record(
        txn_id,
        prev_lsn,
        LogRecordType::Commit,
        LogRecordContent::Transaction(TransactionOperationContent {
            timestamp: 2000,
            metadata: None,
        }),
    )?;
    
    // Flush to disk
    log_manager.flush()?;
    
    // LSNs should be sequential
    assert_eq!(prev_lsn, 3);
    
    Ok(())
}

#[test]
fn test_wal_checkpoint() -> anyhow::Result<()> {
    // Create a log manager
    let (log_manager, _temp_dir) = create_test_log_manager();
    
    // Create a transaction
    let txn_id = 1;
    let mut prev_lsn = 0;
    
    // BEGIN transaction
    prev_lsn = log_manager.append_log_record(
        txn_id,
        prev_lsn,
        LogRecordType::Begin,
        LogRecordContent::Transaction(TransactionOperationContent {
            timestamp: 1000,
            metadata: None,
        }),
    )?;
    
    // Create a checkpoint
    let checkpoint_lsn = log_manager.checkpoint(&[txn_id], &[(1, prev_lsn)])?;
    
    // The checkpoint LSN should be one more than the last transaction LSN
    assert_eq!(checkpoint_lsn, prev_lsn + 1);
    
    // Flush to disk
    log_manager.flush()?;
    
    Ok(())
}

#[test]
fn test_wal_multiple_transactions() -> anyhow::Result<()> {
    // Create a log manager
    let (log_manager, _temp_dir) = create_test_log_manager();
    
    // Handle multiple concurrent transactions
    let txn1_id = 1;
    let txn2_id = 2;
    
    let mut txn1_prev_lsn = 0;
    let mut txn2_prev_lsn = 0;
    
    // BEGIN transaction 1
    txn1_prev_lsn = log_manager.append_log_record(
        txn1_id,
        txn1_prev_lsn,
        LogRecordType::Begin,
        LogRecordContent::Transaction(TransactionOperationContent {
            timestamp: 1000,
            metadata: None,
        }),
    )?;
    
    // BEGIN transaction 2
    txn2_prev_lsn = log_manager.append_log_record(
        txn2_id,
        txn2_prev_lsn,
        LogRecordType::Begin,
        LogRecordContent::Transaction(TransactionOperationContent {
            timestamp: 1001,
            metadata: None,
        }),
    )?;
    
    // INSERT for transaction 1
    txn1_prev_lsn = log_manager.append_log_record(
        txn1_id,
        txn1_prev_lsn,
        LogRecordType::Insert,
        LogRecordContent::Data(DataOperationContent {
            table_id: 1,
            page_id: 1,
            record_id: 100,
            before_image: None,
            after_image: Some(b"Transaction 1 record".to_vec()),
        }),
    )?;
    
    // INSERT for transaction 2
    txn2_prev_lsn = log_manager.append_log_record(
        txn2_id,
        txn2_prev_lsn,
        LogRecordType::Insert,
        LogRecordContent::Data(DataOperationContent {
            table_id: 2,
            page_id: 1,
            record_id: 200,
            before_image: None,
            after_image: Some(b"Transaction 2 record".to_vec()),
        }),
    )?;
    
    // COMMIT transaction 1
    txn1_prev_lsn = log_manager.append_log_record(
        txn1_id,
        txn1_prev_lsn,
        LogRecordType::Commit,
        LogRecordContent::Transaction(TransactionOperationContent {
            timestamp: 2000,
            metadata: None,
        }),
    )?;
    
    // ABORT transaction 2
    txn2_prev_lsn = log_manager.append_log_record(
        txn2_id,
        txn2_prev_lsn,
        LogRecordType::Abort,
        LogRecordContent::Transaction(TransactionOperationContent {
            timestamp: 2001,
            metadata: None,
        }),
    )?;
    
    // Flush to disk
    log_manager.flush()?;
    
    // Transaction 1 should have 3 records (BEGIN, INSERT, COMMIT)
    assert_eq!(txn1_prev_lsn, 5);
    
    // Transaction 2 should have 3 records (BEGIN, INSERT, ABORT)
    assert_eq!(txn2_prev_lsn, 6);
    
    Ok(())
} 