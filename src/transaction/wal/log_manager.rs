// BayunDB WAL Log Manager
//
// This file is a facade that delegates to the refactored components.
// It maintains the original API for backward compatibility.

use std::path::PathBuf;

use crate::transaction::wal::log_components::log_manager_core::{LogManager as CoreLogManager, LogManagerConfig as CoreConfig, LogManagerError as CoreLogManagerError, Result as CoreResult};
use crate::transaction::wal::log_components::log_iterator::{LogRecordIterator as CoreLogRecordIterator, LogManagerIteratorExt};
use crate::transaction::wal::log_record::{LogRecord, LogRecordType, LogRecordContent};

// Define Result type alias
pub type Result<T> = CoreResult<T>;

// Re-export LogManagerError for backward compatibility
pub use crate::transaction::wal::log_components::log_manager_core::LogManagerError;

// Define LogRecordIterator alias
pub type LogRecordIterator = CoreLogRecordIterator;

/// Configuration for the log manager
#[derive(Debug, Clone)]
pub struct LogManagerConfig {
    /// Path to the log directory
    pub log_dir: PathBuf,
    
    /// Base name for log files
    pub log_file_base_name: String,
    
    /// Maximum size of a log file before rotation
    pub max_log_file_size: u64,
    
    /// Log buffer configuration
    pub buffer_config: crate::transaction::wal::log_buffer::LogBufferConfig,
    
    /// Whether to force sync on every commit
    pub force_sync: bool,
}

impl Default for LogManagerConfig {
    fn default() -> Self {
        Self {
            log_dir: PathBuf::from("logs"),
            log_file_base_name: "bayundb_log".to_string(),
            max_log_file_size: 100 * 1024 * 1024, // 100 MB
            buffer_config: crate::transaction::wal::log_buffer::LogBufferConfig::default(),
            force_sync: true,
        }
    }
}

impl From<LogManagerConfig> for CoreConfig {
    fn from(config: LogManagerConfig) -> Self {
        CoreConfig {
            log_dir: config.log_dir,
            log_file_base_name: config.log_file_base_name,
            max_log_file_size: config.max_log_file_size,
            buffer_config: config.buffer_config,
            force_sync: config.force_sync,
        }
    }
}

/// Manager for write-ahead logging operations
pub struct LogManager {
    /// The core log manager implementation
    core: CoreLogManager,
}

impl LogManager {
    /// Create a new log manager with the given configuration
    pub fn new(config: LogManagerConfig) -> Result<Self> {
        let core_config = config.into();
        let core = CoreLogManager::new(core_config)?;
        
        Ok(Self { core })
    }
    
    /// Create a log manager with default configuration
    pub fn default() -> Result<Self> {
        Self::new(LogManagerConfig::default())
    }
    
    /// Get the current LSN
    pub fn current_lsn(&self) -> u64 {
        self.core.current_lsn()
    }
    
    /// Append a log record to the log
    pub fn append_log_record(
        &self,
        txn_id: u32,
        prev_lsn: u64,
        record_type: LogRecordType,
        content: LogRecordContent,
    ) -> Result<u64> {
        self.core.append_log_record(txn_id, prev_lsn, record_type, content)
    }
    
    /// Flush the log buffer to disk
    pub fn flush(&self) -> Result<u64> {
        self.core.flush()
    }
    
    /// Flush the log buffer up to a specific LSN
    pub fn flush_till_lsn(&self, target_lsn: u64) -> Result<()> {
        self.core.flush_till_lsn(target_lsn)
    }
    
    /// Create a checkpoint in the log
    pub fn checkpoint(&self, active_txns: &[u32], dirty_pages: &[(u32, u64)]) -> Result<u64> {
        self.core.checkpoint(active_txns, dirty_pages)
    }
    
    /// Get an iterator over log records starting from a checkpoint
    pub fn get_log_iterator_from_checkpoint(&self) -> Result<LogRecordIterator> {
        self.core.get_log_iterator_from_checkpoint()
    }
    
    /// Get an iterator over log records starting from the given LSN
    pub fn get_log_iterator_from_lsn(&self, start_lsn: u64) -> Result<LogRecordIterator> {
        self.core.get_log_iterator_from_lsn(start_lsn)
    }
    
    /// Append an abort record to the log
    pub fn append_abort_record(&self, txn_id: u32, prev_lsn: u64) -> Result<u64> {
        self.core.append_abort_record(txn_id, prev_lsn)
    }
    
    /// Append a compensation record to the log
    pub fn append_compensation_record(
        &self,
        txn_id: u32,
        undo_next_lsn: u64,
        op_type: LogRecordType,
        table_id: u32,
        page_id: u32,
        record_id: u32,
        before_image: Option<Vec<u8>>,
        after_image: Option<Vec<u8>>,
    ) -> Result<u64> {
        // This is a simplified version of the original method
        // In a real implementation, we would need to properly construct
        // a compensation record and delegate to the core manager
        
        // Create a data content
        let content = LogRecordContent::Data(
            crate::transaction::wal::log_record::DataOperationContent {
                table_id,
                page_id,
                record_id,
                before_image,
                after_image,
            }
        );
        
        // Append using the core manager
        self.core.append_log_record(txn_id, undo_next_lsn, op_type, content)
    }
    
    /// Get a specific log record by LSN
    pub fn get_log_record(&self, lsn: u64) -> Result<LogRecord> {
        // Create an iterator starting from this LSN
        let mut iterator = self.core.get_log_iterator_from_lsn(lsn)?;
        
        // Get the first record
        match iterator.next() {
            Some(Ok(record)) => {
                // Check if it's the record we're looking for
                if record.lsn == lsn {
                    Ok(record)
                } else {
                    Err(CoreLogManagerError::InvalidState(format!("Record with LSN {} not found", lsn)))
                }
            },
            Some(Err(e)) => Err(e),
            None => Err(CoreLogManagerError::InvalidState(format!("Record with LSN {} not found", lsn))),
        }
    }
    
    /// Get all log records for a specific transaction
    pub fn get_transaction_records(&self, txn_id: u32) -> Result<Vec<LogRecord>> {
        // Create an iterator from the beginning
        let iterator = self.core.get_log_iterator_from_lsn(0)?;
        
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
    
    /// Abort a transaction, writing necessary records
    pub fn abort_transaction(&self, txn_id: u32, prev_lsn: u64) -> Result<u64> {
        // Simply delegate to append_abort_record
        self.append_abort_record(txn_id, prev_lsn)
    }
}

impl Clone for LogManager {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
        }
    }
} 