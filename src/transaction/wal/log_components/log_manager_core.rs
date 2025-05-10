// BayunDB WAL Log Manager Core
//
// This module contains the core functionality for the Write-Ahead Log manager.

use std::sync::{Mutex, atomic::{AtomicU64, Ordering}};
use thiserror::Error;

use crate::transaction::wal::log_buffer::{LogBuffer, LogBufferConfig, LogBufferError};
use crate::transaction::wal::log_record::{LogRecord, LogRecordType, LogRecordContent};
use crate::transaction::wal::log_components::log_file_manager::{LogFileManager, LogFileError};
use crate::transaction::wal::log_components::log_iterator::LogManagerIteratorExt;

/// Error type for log manager operations
#[derive(Error, Debug)]
pub enum LogManagerError {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("Buffer error: {0}")]
    BufferError(#[from] LogBufferError),
    
    #[error("Log record error: {0}")]
    LogRecordError(#[from] crate::transaction::wal::log_record::LogRecordError),
    
    #[error("File manager error: {0}")]
    FileError(#[from] LogFileError),
    
    #[error("Invalid log file format")]
    InvalidFormat,
    
    #[error("Invalid log state: {0}")]
    InvalidState(String),
}

/// Result type for log manager operations
pub type Result<T> = std::result::Result<T, LogManagerError>;

/// Configuration for the log manager
#[derive(Debug, Clone)]
pub struct LogManagerConfig {
    /// Path to the log directory
    pub log_dir: std::path::PathBuf,
    
    /// Base name for log files
    pub log_file_base_name: String,
    
    /// Maximum size of a log file before rotation
    pub max_log_file_size: u64,
    
    /// Log buffer configuration
    pub buffer_config: LogBufferConfig,
    
    /// Whether to force sync on every commit
    pub force_sync: bool,
}

impl Default for LogManagerConfig {
    fn default() -> Self {
        Self {
            log_dir: std::path::PathBuf::from("logs"),
            log_file_base_name: "bayundb_log".to_string(),
            max_log_file_size: 100 * 1024 * 1024, // 100 MB
            buffer_config: LogBufferConfig::default(),
            force_sync: true,
        }
    }
}

/// Manager for write-ahead logging operations
pub struct LogManager {
    /// Configuration for the log manager
    config: LogManagerConfig,
    
    /// File manager for log file operations
    file_manager: LogFileManager,
    
    /// Current LSN to assign to new log records
    current_lsn: AtomicU64,
    
    /// In-memory buffer for log records
    log_buffer: LogBuffer,
    
    /// Flag indicating if the log manager is flushing
    is_flushing: Mutex<bool>,
}

impl LogManager {
    /// Create a new log manager with the given configuration
    pub fn new(config: LogManagerConfig) -> Result<Self> {
        // Create log directory if it doesn't exist
        std::fs::create_dir_all(&config.log_dir)?;
        
        // Initialize the file manager
        let (file_manager, current_lsn) = LogFileManager::new(&config)?;
        
        // Create the log buffer
        let log_buffer = LogBuffer::new(config.buffer_config.clone());
        
        Ok(Self {
            config,
            file_manager,
            current_lsn: AtomicU64::new(current_lsn),
            log_buffer,
            is_flushing: Mutex::new(false),
        })
    }
    
    /// Create a log manager with default configuration
    pub fn default() -> Result<Self> {
        Self::new(LogManagerConfig::default())
    }
    
    /// Get the current LSN
    pub fn current_lsn(&self) -> u64 {
        self.current_lsn.load(Ordering::SeqCst)
    }
    
    /// Append a log record to the log
    pub fn append_log_record(
        &self,
        txn_id: u32,
        prev_lsn: u64,
        record_type: LogRecordType,
        content: LogRecordContent,
    ) -> Result<u64> {
        // Assign an LSN
        let lsn = self.current_lsn.fetch_add(1, Ordering::SeqCst);
        
        // Create the log record
        let record = LogRecord::new(lsn, txn_id, prev_lsn, record_type, content);
        
        // Append to the log buffer
        self.log_buffer.append(&record)?;
        
        // If we need to flush immediately for COMMIT records or due to configuration
        if record_type == LogRecordType::Commit && self.config.force_sync {
            self.flush()?;
        }
        
        Ok(lsn)
    }
    
    /// Flush the log buffer to disk
    pub fn flush(&self) -> Result<u64> {
        // Try to acquire the flush lock
        let mut flushing = match self.is_flushing.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                // Another thread is already flushing
                return Ok(self.current_lsn());
            }
        };
        
        // Set the flushing flag to true
        *flushing = true;
        
        // Flush the log buffer to disk, providing a writer function
        let max_lsn = self.log_buffer.flush(|data| {
            self.file_manager.write_data(data)
        })?;
        
        // Reset the flushing flag
        *flushing = false;
        
        Ok(max_lsn)
    }
    
    /// Flush the log buffer up to a specific LSN
    pub fn flush_till_lsn(&self, target_lsn: u64) -> Result<()> {
        if target_lsn <= self.flush()? {
            Ok(())
        } else {
            Err(LogManagerError::InvalidState(
                format!("Cannot flush up to LSN {}", target_lsn)
            ))
        }
    }
    
    /// Create a checkpoint in the log
    pub fn checkpoint(&self, active_txns: &[u32], dirty_pages: &[(u32, u64)]) -> Result<u64> {
        // Create a checkpoint record
        let checkpoint_record = LogRecord::new_checkpoint(
            self.current_lsn.fetch_add(1, Ordering::SeqCst),
            active_txns.to_vec(),
            dirty_pages.to_vec(),
        );
        
        // Append to the log buffer
        self.log_buffer.append(&checkpoint_record)?;
        
        // Force flush the buffer
        self.flush()?;
        
        Ok(checkpoint_record.lsn)
    }
    
    /// Append an abort record to the log
    pub fn append_abort_record(&self, txn_id: u32, prev_lsn: u64) -> Result<u64> {
        self.append_log_record(
            txn_id,
            prev_lsn,
            LogRecordType::Abort,
            LogRecordContent::Transaction(
                crate::transaction::wal::log_record::TransactionOperationContent {
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    metadata: None,
                }
            )
        )
    }
    
    /// Get the file manager instance
    pub fn file_manager(&self) -> &LogFileManager {
        &self.file_manager
    }
    
    /// Get all log records for a specific transaction
    pub fn get_transaction_records(&self, txn_id: u32) -> Result<Vec<LogRecord>> {
        // Create an iterator from the beginning
        let iterator = self.get_log_iterator_from_lsn(0)?;
        
        // Collect all records for this transaction
        let mut records = Vec::new();
        
        for record_result in iterator {
            match record_result {
                Ok(record) => {
                    if record.txn_id == txn_id {
                        records.push(record);
                    }
                },
                Err(e) => return Err(e),
            }
        }
        
        Ok(records)
    }
}

impl Clone for LogManager {
    fn clone(&self) -> Self {
        let current_lsn = self.current_lsn.load(Ordering::SeqCst);
        let is_flushing = self.is_flushing.lock().unwrap().clone();
        
        Self {
            config: self.config.clone(),
            file_manager: self.file_manager.clone(),
            current_lsn: AtomicU64::new(current_lsn),
            log_buffer: self.log_buffer.clone(),
            is_flushing: Mutex::new(is_flushing),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    fn create_test_log_dir() -> TempDir {
        TempDir::new().unwrap()
    }
    
    #[test]
    fn test_log_manager_creation() {
        let log_dir = create_test_log_dir();
        
        let config = LogManagerConfig {
            log_dir: log_dir.path().to_path_buf(),
            log_file_base_name: "test_log".to_string(),
            max_log_file_size: 1024 * 1024,
            buffer_config: LogBufferConfig {
                buffer_size: 4096,
                flush_threshold: 0.75,
            },
            force_sync: true,
        };
        
        let log_manager = LogManager::new(config).unwrap();
        assert_eq!(log_manager.current_lsn(), 0);
    }
    
    #[test]
    fn test_log_record_append() {
        let log_dir = create_test_log_dir();
        
        let config = LogManagerConfig {
            log_dir: log_dir.path().to_path_buf(),
            log_file_base_name: "test_log".to_string(),
            max_log_file_size: 1024 * 1024,
            buffer_config: LogBufferConfig {
                buffer_size: 4096,
                flush_threshold: 0.75,
            },
            force_sync: true,
        };
        
        let log_manager = LogManager::new(config).unwrap();
        
        // Append a BEGIN record
        let lsn1 = log_manager.append_log_record(
            1, 
            0,
            LogRecordType::Begin,
            LogRecordContent::Transaction(
                crate::transaction::wal::log_record::TransactionOperationContent {
                    timestamp: 0,
                    metadata: None,
                }
            )
        ).unwrap();
        
        assert_eq!(lsn1, 0);
        
        // Append an UPDATE record
        let lsn2 = log_manager.append_log_record(
            1,
            lsn1,
            LogRecordType::Update,
            LogRecordContent::Data(
                crate::transaction::wal::log_record::DataOperationContent {
                    table_id: 1,
                    page_id: 1,
                    record_id: 1,
                    before_image: Some(vec![1, 2, 3]),
                    after_image: Some(vec![4, 5, 6]),
                }
            )
        ).unwrap();
        
        assert_eq!(lsn2, 1);
    }
} 