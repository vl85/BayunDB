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