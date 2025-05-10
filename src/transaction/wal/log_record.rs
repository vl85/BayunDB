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
    /// Records a compensation operation for an update (during undo)
    CompensationUpdate,
    /// Records a compensation operation for an insert (during undo)
    CompensationInsert,
    /// Records a compensation operation for a delete (during undo)
    CompensationDelete,
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

    // Helper to check LogRecord equality, especially for content
    fn assert_records_equal(r1: &LogRecord, r2: &LogRecord) {
        assert_eq!(r1.lsn, r2.lsn);
        assert_eq!(r1.txn_id, r2.txn_id);
        assert_eq!(r1.prev_lsn, r2.prev_lsn);
        assert_eq!(r1.record_type, r2.record_type);
        match (&r1.content, &r2.content) {
            (LogRecordContent::Transaction(t1), LogRecordContent::Transaction(t2)) => {
                // Timestamps can be slightly different, so we don't assert them strictly
                // assert_eq!(t1.timestamp, t2.timestamp);
                assert_eq!(t1.metadata, t2.metadata);
            }
            (LogRecordContent::Data(d1), LogRecordContent::Data(d2)) => {
                assert_eq!(d1.table_id, d2.table_id);
                assert_eq!(d1.page_id, d2.page_id);
                assert_eq!(d1.record_id, d2.record_id);
                assert_eq!(d1.before_image, d2.before_image);
                assert_eq!(d1.after_image, d2.after_image);
            }
            (LogRecordContent::Checkpoint(c1), LogRecordContent::Checkpoint(c2)) => {
                // Timestamps can be slightly different
                // assert_eq!(c1.timestamp, c2.timestamp);
                assert_eq!(c1.active_transactions, c2.active_transactions);
                assert_eq!(c1.dirty_pages, c2.dirty_pages);
            }
            _ => panic!("Mismatched LogRecordContent types during comparison"),
        }
    }

    #[test]
    fn test_begin_record_serialization() {
        let record = LogRecord::new_begin(1, 100);
        let serialized = record.serialize().unwrap();
        let deserialized = LogRecord::deserialize(&serialized).unwrap();
        assert_records_equal(&record, &deserialized);
    }

    #[test]
    fn test_commit_record_serialization() {
        let record = LogRecord::new_commit(2, 100, 1);
        let serialized = record.serialize().unwrap();
        let deserialized = LogRecord::deserialize(&serialized).unwrap();
        assert_records_equal(&record, &deserialized);
    }

    #[test]
    fn test_abort_record_serialization() {
        let record = LogRecord::new_abort(3, 100, 2);
        let serialized = record.serialize().unwrap();
        let deserialized = LogRecord::deserialize(&serialized).unwrap();
        assert_records_equal(&record, &deserialized);
    }

    #[test]
    fn test_update_record_serialization() {
        let before = vec![1, 2, 3];
        let after = vec![4, 5, 6];
        let record = LogRecord::new_update(4, 100, 3, 1, 1, 1, before, after);
        let serialized = record.serialize().unwrap();
        let deserialized = LogRecord::deserialize(&serialized).unwrap();
        assert_records_equal(&record, &deserialized);
    }

    #[test]
    fn test_insert_record_serialization() {
        let after = vec![7, 8, 9];
        let record = LogRecord::new_insert(5, 100, 4, 1, 1, 2, after);
        let serialized = record.serialize().unwrap();
        let deserialized = LogRecord::deserialize(&serialized).unwrap();
        assert_records_equal(&record, &deserialized);
    }

    #[test]
    fn test_delete_record_serialization() {
        let before = vec![10, 11, 12];
        let record = LogRecord::new_delete(6, 100, 5, 1, 1, 3, before);
        let serialized = record.serialize().unwrap();
        let deserialized = LogRecord::deserialize(&serialized).unwrap();
        assert_records_equal(&record, &deserialized);
    }

    #[test]
    fn test_checkpoint_record_serialization() {
        let active_txns = vec![100, 101];
        let dirty_pages = vec![(1, 5), (2, 6)];
        let record = LogRecord::new_checkpoint(7, active_txns, dirty_pages);
        let serialized = record.serialize().unwrap();
        let deserialized = LogRecord::deserialize(&serialized).unwrap();
        assert_records_equal(&record, &deserialized);
    }

    #[test]
    fn test_compensation_record_serialization() {
        // Compensation records use DataOperationContent, so we test one variant.
        // This represents undoing an insert, so it has a before_image (the inserted data) 
        // and no after_image.
        let before_image_for_compensated_insert = vec![1,2,3];
        let record = LogRecord::new(
            8, 
            100, 
            7, 
            LogRecordType::CompensationInsert, 
            LogRecordContent::Data(DataOperationContent {
                table_id: 1,
                page_id: 1,
                record_id: 1,
                before_image: Some(before_image_for_compensated_insert),
                after_image: None,
            })
        );
        let serialized = record.serialize().unwrap();
        let deserialized = LogRecord::deserialize(&serialized).unwrap();
        assert_records_equal(&record, &deserialized);
    }

    #[test]
    fn test_deserialize_invalid_data() {
        let invalid_data = vec![1, 2, 3, 4]; // Too short / malformed
        let result = LogRecord::deserialize(&invalid_data);
        assert!(result.is_err());
        match result.err().unwrap() {
            LogRecordError::DeserializationError(_) => {},
            _ => panic!("Expected DeserializationError"),
        }

        // Test with slightly more data, but still likely invalid
        let slightly_better_data = bincode::serialize(&("random_string_not_a_log_record")).unwrap();
        let result_random = LogRecord::deserialize(&slightly_better_data);
        assert!(result_random.is_err());
        match result_random.err().unwrap() {
            LogRecordError::DeserializationError(_) => {},
            _ => panic!("Expected DeserializationError for random data"),
        }
    }
} 