use thiserror::Error;
use serde::{Serialize, Deserialize};
use bincode::{serialize, deserialize};

/// Error type for log record operations
#[derive(Error, Debug)]
pub enum LogRecordError {
    #[error("Failed to serialize log record: {0}")]
    SerializationError(String),
    
    #[error("Failed to deserialize log record: {0}")]
    DeserializationError(String),
    
    #[error("Invalid log record format")]
    InvalidFormat,
}

/// Result type for log record operations
pub type Result<T> = std::result::Result<T, LogRecordError>;

/// Types of log records supported by the WAL system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogRecordType {
    /// Marks the beginning of a transaction
    Begin,
    /// Marks the successful completion of a transaction
    Commit,
    /// Marks the abortion/rollback of a transaction
    Abort,
    /// Records an update operation (modifying existing data)
    Update,
    /// Records an insert operation (adding new data)
    Insert,
    /// Records a delete operation (removing existing data)
    Delete,
    /// Marks a checkpoint in the log
    Checkpoint,
}

/// Structure representing the content of a data operation log record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataOperationContent {
    /// ID of the table where the operation occurred
    pub table_id: u32,
    /// ID of the page where the operation occurred
    pub page_id: u32,
    /// ID of the record that was affected
    pub record_id: u32,
    /// Before-image of the data (for UPDATE/DELETE)
    pub before_image: Option<Vec<u8>>,
    /// After-image of the data (for INSERT/UPDATE)
    pub after_image: Option<Vec<u8>>,
}

/// Structure representing the content of a transaction operation log record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionOperationContent {
    /// Timestamp when the operation occurred
    pub timestamp: u64,
    /// Additional metadata for the transaction operation
    pub metadata: Option<Vec<u8>>,
}

/// Structure representing the content of a checkpoint log record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointContent {
    /// Timestamp when the checkpoint occurred
    pub timestamp: u64,
    /// List of active transactions at checkpoint time
    pub active_transactions: Vec<u32>,
    /// List of dirty pages at checkpoint time (page_id, lsn)
    pub dirty_pages: Vec<(u32, u64)>,
}

/// Union of all possible log record content types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogRecordContent {
    Data(DataOperationContent),
    Transaction(TransactionOperationContent),
    Checkpoint(CheckpointContent),
}

/// Main log record structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRecord {
    /// Log Sequence Number - unique identifier for this log record
    pub lsn: u64,
    /// Transaction ID that generated this log record
    pub txn_id: u32,
    /// LSN of the previous log record in this transaction
    pub prev_lsn: u64,
    /// Type of operation this log record represents
    pub record_type: LogRecordType,
    /// Content of the log record, depends on the record type
    pub content: LogRecordContent,
}

impl LogRecord {
    /// Create a new log record
    pub fn new(
        lsn: u64,
        txn_id: u32,
        prev_lsn: u64,
        record_type: LogRecordType,
        content: LogRecordContent,
    ) -> Self {
        Self {
            lsn,
            txn_id,
            prev_lsn,
            record_type,
            content,
        }
    }

    /// Create a new BEGIN transaction log record
    pub fn new_begin(lsn: u64, txn_id: u32) -> Self {
        Self::new(
            lsn,
            txn_id,
            0, // No previous LSN for BEGIN
            LogRecordType::Begin,
            LogRecordContent::Transaction(TransactionOperationContent {
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                metadata: None,
            }),
        )
    }

    /// Create a new COMMIT transaction log record
    pub fn new_commit(lsn: u64, txn_id: u32, prev_lsn: u64) -> Self {
        Self::new(
            lsn,
            txn_id,
            prev_lsn,
            LogRecordType::Commit,
            LogRecordContent::Transaction(TransactionOperationContent {
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                metadata: None,
            }),
        )
    }

    /// Create a new ABORT transaction log record
    pub fn new_abort(lsn: u64, txn_id: u32, prev_lsn: u64) -> Self {
        Self::new(
            lsn,
            txn_id,
            prev_lsn,
            LogRecordType::Abort,
            LogRecordContent::Transaction(TransactionOperationContent {
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                metadata: None,
            }),
        )
    }

    /// Create a new UPDATE operation log record
    pub fn new_update(
        lsn: u64,
        txn_id: u32,
        prev_lsn: u64,
        table_id: u32,
        page_id: u32,
        record_id: u32,
        before_image: Vec<u8>,
        after_image: Vec<u8>,
    ) -> Self {
        Self::new(
            lsn,
            txn_id,
            prev_lsn,
            LogRecordType::Update,
            LogRecordContent::Data(DataOperationContent {
                table_id,
                page_id,
                record_id,
                before_image: Some(before_image),
                after_image: Some(after_image),
            }),
        )
    }

    /// Create a new INSERT operation log record
    pub fn new_insert(
        lsn: u64,
        txn_id: u32,
        prev_lsn: u64,
        table_id: u32,
        page_id: u32,
        record_id: u32,
        after_image: Vec<u8>,
    ) -> Self {
        Self::new(
            lsn,
            txn_id,
            prev_lsn,
            LogRecordType::Insert,
            LogRecordContent::Data(DataOperationContent {
                table_id,
                page_id,
                record_id,
                before_image: None,
                after_image: Some(after_image),
            }),
        )
    }

    /// Create a new DELETE operation log record
    pub fn new_delete(
        lsn: u64,
        txn_id: u32,
        prev_lsn: u64,
        table_id: u32,
        page_id: u32,
        record_id: u32,
        before_image: Vec<u8>,
    ) -> Self {
        Self::new(
            lsn,
            txn_id,
            prev_lsn,
            LogRecordType::Delete,
            LogRecordContent::Data(DataOperationContent {
                table_id,
                page_id,
                record_id,
                before_image: Some(before_image),
                after_image: None,
            }),
        )
    }

    /// Create a new CHECKPOINT log record
    pub fn new_checkpoint(
        lsn: u64,
        active_transactions: Vec<u32>,
        dirty_pages: Vec<(u32, u64)>,
    ) -> Self {
        Self::new(
            lsn,
            0, // Checkpoint doesn't belong to any transaction
            0, // No previous LSN for checkpoint
            LogRecordType::Checkpoint,
            LogRecordContent::Checkpoint(CheckpointContent {
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                active_transactions,
                dirty_pages,
            }),
        )
    }

    /// Serialize the log record to bytes
    pub fn serialize(&self) -> Result<Vec<u8>> {
        serialize(self).map_err(|e| LogRecordError::SerializationError(e.to_string()))
    }

    /// Deserialize bytes into a log record
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        deserialize(data).map_err(|e| LogRecordError::DeserializationError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_begin_record_serialization() {
        let record = LogRecord::new_begin(1, 2);
        let serialized = record.serialize().unwrap();
        let deserialized = LogRecord::deserialize(&serialized).unwrap();
        
        assert_eq!(deserialized.lsn, 1);
        assert_eq!(deserialized.txn_id, 2);
        assert_eq!(deserialized.prev_lsn, 0);
        assert_eq!(deserialized.record_type, LogRecordType::Begin);
    }

    #[test]
    fn test_commit_record_serialization() {
        let record = LogRecord::new_commit(5, 2, 3);
        let serialized = record.serialize().unwrap();
        let deserialized = LogRecord::deserialize(&serialized).unwrap();
        
        assert_eq!(deserialized.lsn, 5);
        assert_eq!(deserialized.txn_id, 2);
        assert_eq!(deserialized.prev_lsn, 3);
        assert_eq!(deserialized.record_type, LogRecordType::Commit);
    }

    #[test]
    fn test_update_record_serialization() {
        let record = LogRecord::new_update(
            10, 5, 8, 1, 2, 3, 
            vec![1, 2, 3], 
            vec![4, 5, 6]
        );
        let serialized = record.serialize().unwrap();
        let deserialized = LogRecord::deserialize(&serialized).unwrap();
        
        assert_eq!(deserialized.lsn, 10);
        assert_eq!(deserialized.txn_id, 5);
        assert_eq!(deserialized.prev_lsn, 8);
        assert_eq!(deserialized.record_type, LogRecordType::Update);
        
        if let LogRecordContent::Data(content) = deserialized.content {
            assert_eq!(content.table_id, 1);
            assert_eq!(content.page_id, 2);
            assert_eq!(content.record_id, 3);
            assert_eq!(content.before_image.unwrap(), vec![1, 2, 3]);
            assert_eq!(content.after_image.unwrap(), vec![4, 5, 6]);
        } else {
            panic!("Unexpected content type!");
        }
    }
} 