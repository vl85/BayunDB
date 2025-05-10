// BayunDB Transaction implementation
// Represents an active database transaction

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

use crate::common::types::{TxnId, Lsn};
use crate::transaction::wal::log_manager::LogManager;

/// Transaction isolation levels
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// Transaction states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

/// Errors that can occur during transaction processing
#[derive(Error, Debug)]
pub enum TransactionError {
    #[error("Transaction {0} is already committed or aborted")]
    InvalidState(TxnId),
    
    #[error("Failed to write to WAL: {0}")]
    LogError(String),
    
    #[error("Internal transaction error: {0}")]
    InternalError(String),
}

/// Result type for transaction operations
pub type Result<T> = std::result::Result<T, TransactionError>;

/// Transaction - represents an active database transaction
pub struct Transaction {
    /// Transaction ID
    id: TxnId,
    
    /// Current transaction state
    state: TransactionState,
    
    /// Isolation level for this transaction
    isolation_level: IsolationLevel,
    
    /// Log manager reference
    log_manager: Arc<LogManager>,
    
    /// First LSN of this transaction
    first_lsn: AtomicU64,
    
    /// Last LSN of this transaction
    last_lsn: AtomicU64,
}

impl Clone for Transaction {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            state: self.state,
            isolation_level: self.isolation_level,
            log_manager: self.log_manager.clone(),
            first_lsn: AtomicU64::new(self.first_lsn.load(Ordering::SeqCst)),
            last_lsn: AtomicU64::new(self.last_lsn.load(Ordering::SeqCst)),
        }
    }
}

impl Transaction {
    /// Create a new transaction with the given ID
    pub fn new(txn_id: TxnId, log_manager: Arc<LogManager>, isolation_level: IsolationLevel) -> Self {
        Self {
            id: txn_id,
            state: TransactionState::Active,
            isolation_level,
            log_manager,
            first_lsn: AtomicU64::new(0),
            last_lsn: AtomicU64::new(0),
        }
    }
    
    /// Begin the transaction by writing a BEGIN record to the log
    pub fn begin(&self) -> Result<Lsn> {
        use crate::transaction::wal::log_record::{LogRecordType, LogRecordContent, TransactionOperationContent};
        
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
            
        let lsn = match self.log_manager.append_log_record(
            self.id,
            0, // prev_lsn is 0 for BEGIN
            LogRecordType::Begin,
            LogRecordContent::Transaction(TransactionOperationContent {
                timestamp,
                metadata: None,
            }),
        ) {
            Ok(lsn) => lsn,
            Err(e) => return Err(TransactionError::LogError(e.to_string())),
        };
        
        // Set first and last LSN
        self.first_lsn.store(lsn, Ordering::SeqCst);
        self.last_lsn.store(lsn, Ordering::SeqCst);
        
        Ok(lsn)
    }
    
    /// Commit the transaction
    pub fn commit(&mut self) -> Result<Lsn> {
        if self.state != TransactionState::Active {
            return Err(TransactionError::InvalidState(self.id));
        }
        
        use crate::transaction::wal::log_record::{LogRecordType, LogRecordContent, TransactionOperationContent};
        
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
            
        let prev_lsn = self.last_lsn.load(Ordering::SeqCst);
            
        let lsn = match self.log_manager.append_log_record(
            self.id,
            prev_lsn,
            LogRecordType::Commit,
            LogRecordContent::Transaction(TransactionOperationContent {
                timestamp,
                metadata: None,
            }),
        ) {
            Ok(lsn) => lsn,
            Err(e) => return Err(TransactionError::LogError(e.to_string())),
        };
        
        // Update last LSN
        self.last_lsn.store(lsn, Ordering::SeqCst);
        
        // Update state
        self.state = TransactionState::Committed;
        
        Ok(lsn)
    }
    
    /// Abort the transaction
    pub fn abort(&mut self) -> Result<Lsn> {
        if self.state != TransactionState::Active {
            return Err(TransactionError::InvalidState(self.id));
        }
        
        use crate::transaction::wal::log_record::{LogRecordType, LogRecordContent, TransactionOperationContent};
        
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
            
        let prev_lsn = self.last_lsn.load(Ordering::SeqCst);
            
        let lsn = match self.log_manager.append_log_record(
            self.id,
            prev_lsn,
            LogRecordType::Abort,
            LogRecordContent::Transaction(TransactionOperationContent {
                timestamp,
                metadata: None,
            }),
        ) {
            Ok(lsn) => lsn,
            Err(e) => return Err(TransactionError::LogError(e.to_string())),
        };
        
        // Update last LSN
        self.last_lsn.store(lsn, Ordering::SeqCst);
        
        // Update state
        self.state = TransactionState::Aborted;
        
        Ok(lsn)
    }
    
    /// Record a log for an insert operation
    pub fn log_insert(&self, table_id: u32, page_id: u32, record_id: u32, data: &[u8]) -> Result<Lsn> {
        if self.state != TransactionState::Active {
            return Err(TransactionError::InvalidState(self.id));
        }
        
        use crate::transaction::wal::log_record::{LogRecordType, LogRecordContent, DataOperationContent};
        
        let prev_lsn = self.last_lsn.load(Ordering::SeqCst);
            
        let lsn = match self.log_manager.append_log_record(
            self.id,
            prev_lsn,
            LogRecordType::Insert,
            LogRecordContent::Data(DataOperationContent {
                table_id,
                page_id,
                record_id,
                before_image: None,
                after_image: Some(data.to_vec()),
            }),
        ) {
            Ok(lsn) => lsn,
            Err(e) => return Err(TransactionError::LogError(e.to_string())),
        };
        
        // Update last LSN
        self.last_lsn.store(lsn, Ordering::SeqCst);
        
        Ok(lsn)
    }
    
    /// Record a log for an update operation
    pub fn log_update(&self, table_id: u32, page_id: u32, record_id: u32, 
                     before_data: &[u8], after_data: &[u8]) -> Result<Lsn> {
        if self.state != TransactionState::Active {
            return Err(TransactionError::InvalidState(self.id));
        }
        
        use crate::transaction::wal::log_record::{LogRecordType, LogRecordContent, DataOperationContent};
        
        let prev_lsn = self.last_lsn.load(Ordering::SeqCst);
            
        let lsn = match self.log_manager.append_log_record(
            self.id,
            prev_lsn,
            LogRecordType::Update,
            LogRecordContent::Data(DataOperationContent {
                table_id,
                page_id,
                record_id,
                before_image: Some(before_data.to_vec()),
                after_image: Some(after_data.to_vec()),
            }),
        ) {
            Ok(lsn) => lsn,
            Err(e) => return Err(TransactionError::LogError(e.to_string())),
        };
        
        // Update last LSN
        self.last_lsn.store(lsn, Ordering::SeqCst);
        
        Ok(lsn)
    }
    
    /// Record a log for a delete operation
    pub fn log_delete(&self, table_id: u32, page_id: u32, record_id: u32, before_data: &[u8]) -> Result<Lsn> {
        if self.state != TransactionState::Active {
            return Err(TransactionError::InvalidState(self.id));
        }
        
        use crate::transaction::wal::log_record::{LogRecordType, LogRecordContent, DataOperationContent};
        
        let prev_lsn = self.last_lsn.load(Ordering::SeqCst);
            
        let lsn = match self.log_manager.append_log_record(
            self.id,
            prev_lsn,
            LogRecordType::Delete,
            LogRecordContent::Data(DataOperationContent {
                table_id,
                page_id,
                record_id,
                before_image: Some(before_data.to_vec()),
                after_image: None,
            }),
        ) {
            Ok(lsn) => lsn,
            Err(e) => return Err(TransactionError::LogError(e.to_string())),
        };
        
        // Update last LSN
        self.last_lsn.store(lsn, Ordering::SeqCst);
        
        Ok(lsn)
    }
    
    /// Get transaction ID
    pub fn id(&self) -> TxnId {
        self.id
    }
    
    /// Get transaction state
    pub fn state(&self) -> TransactionState {
        self.state
    }
    
    /// Get transaction isolation level
    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }
    
    /// Get the first LSN of this transaction
    pub fn first_lsn(&self) -> Lsn {
        self.first_lsn.load(Ordering::SeqCst)
    }
    
    /// Get the last LSN of this transaction
    pub fn last_lsn(&self) -> Lsn {
        self.last_lsn.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::wal::log_manager::LogManagerConfig;
    use tempfile::TempDir;
    use std::path::PathBuf;

    fn get_test_log_manager() -> Arc<LogManager> {
        let temp_dir = TempDir::new().unwrap();
        let mut config = LogManagerConfig::default();
        config.log_dir = PathBuf::from(temp_dir.path());
        // Keep temp_dir alive by leaking it or returning it.
        // For simplicity in this test, we'll leak it. This is not ideal for non-test code.
        std::mem::forget(temp_dir); 
        Arc::new(LogManager::new(config).unwrap())
    }

    #[test]
    fn test_transaction_new() {
        let log_manager = get_test_log_manager();
        let txn = Transaction::new(1, log_manager, IsolationLevel::ReadCommitted);
        assert_eq!(txn.id(), 1);
        assert_eq!(txn.state(), TransactionState::Active);
        assert_eq!(txn.isolation_level(), IsolationLevel::ReadCommitted);
        assert_eq!(txn.first_lsn(), 0);
        assert_eq!(txn.last_lsn(), 0);
    }

    #[test]
    fn test_transaction_begin() {
        let log_manager = get_test_log_manager();
        let txn = Transaction::new(2, log_manager, IsolationLevel::RepeatableRead);
        
        let begin_lsn_result = txn.begin();
        assert!(begin_lsn_result.is_ok());
        let begin_lsn = begin_lsn_result.unwrap();
        
        assert!(begin_lsn > 0, "Begin LSN should be greater than 0");
        assert_eq!(txn.first_lsn(), begin_lsn);
        assert_eq!(txn.last_lsn(), begin_lsn);
        assert_eq!(txn.state(), TransactionState::Active); // State remains active after begin
    }

    #[test]
    fn test_transaction_commit() {
        let log_manager = get_test_log_manager();
        let mut txn = Transaction::new(3, log_manager, IsolationLevel::Serializable);
        
        let begin_lsn = txn.begin().unwrap();
        
        let commit_lsn_result = txn.commit();
        assert!(commit_lsn_result.is_ok());
        let commit_lsn = commit_lsn_result.unwrap();

        assert!(commit_lsn > begin_lsn, "Commit LSN should be greater than begin LSN");
        assert_eq!(txn.last_lsn(), commit_lsn);
        assert_eq!(txn.state(), TransactionState::Committed);

        // Try to commit again
        let commit_again_result = txn.commit();
        assert!(commit_again_result.is_err());
        if let Err(TransactionError::InvalidState(id)) = commit_again_result {
            assert_eq!(id, 3);
        } else {
            panic!("Expected InvalidState error");
        }
    }

    #[test]
    fn test_transaction_abort() {
        let log_manager = get_test_log_manager();
        let mut txn = Transaction::new(4, log_manager, IsolationLevel::ReadUncommitted);
        
        let begin_lsn = txn.begin().unwrap();

        let abort_lsn_result = txn.abort();
        assert!(abort_lsn_result.is_ok());
        let abort_lsn = abort_lsn_result.unwrap();

        assert!(abort_lsn > begin_lsn, "Abort LSN should be greater than begin LSN");
        assert_eq!(txn.last_lsn(), abort_lsn);
        assert_eq!(txn.state(), TransactionState::Aborted);

        // Try to abort again
        let abort_again_result = txn.abort();
        assert!(abort_again_result.is_err());
        if let Err(TransactionError::InvalidState(id)) = abort_again_result {
            assert_eq!(id, 4);
        } else {
            panic!("Expected InvalidState error");
        }
    }

    #[test]
    fn test_transaction_log_operations_state() {
        let log_manager = get_test_log_manager();
        let mut txn = Transaction::new(5, log_manager.clone(), IsolationLevel::ReadCommitted);
        
        // Operations should fail before begin (though begin is usually called by TransactionManager)
        // For this unit test, we'll assume begin has been called.
        txn.begin().unwrap();

        let data = [1, 2, 3];
        let log_insert_res = txn.log_insert(1,1,1, &data);
        assert!(log_insert_res.is_ok());
        let insert_lsn = log_insert_res.unwrap();
        assert!(txn.last_lsn() >= insert_lsn);

        let before_data = [4,5,6];
        let after_data = [7,8,9];
        let log_update_res = txn.log_update(1,1,1, &before_data, &after_data);
        assert!(log_update_res.is_ok());
        let update_lsn = log_update_res.unwrap();
         assert!(txn.last_lsn() >= update_lsn);

        let log_delete_res = txn.log_delete(1,1,1, &before_data);
        assert!(log_delete_res.is_ok());
        let delete_lsn = log_delete_res.unwrap();
        assert!(txn.last_lsn() >= delete_lsn);

        // Commit transaction
        txn.commit().unwrap();
        assert_eq!(txn.state(), TransactionState::Committed);

        // Log operations should fail after commit
        assert!(txn.log_insert(1,1,1, &data).is_err());
        assert!(txn.log_update(1,1,1, &before_data, &after_data).is_err());
        assert!(txn.log_delete(1,1,1, &before_data).is_err());

        // Test on an aborted transaction
        let mut txn2 = Transaction::new(6, log_manager, IsolationLevel::ReadCommitted);
        txn2.begin().unwrap();
        txn2.abort().unwrap();
        assert_eq!(txn2.state(), TransactionState::Aborted);
        assert!(txn2.log_insert(1,1,1, &data).is_err());
    }
} 