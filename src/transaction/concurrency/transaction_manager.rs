use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU32, Ordering};

use crate::common::types::TxnId;
use crate::transaction::{Transaction, IsolationLevel, TransactionError};
use crate::transaction::wal::log_manager::LogManager;

/// Transaction manager - responsible for creating and tracking transactions
pub struct TransactionManager {
    /// Next transaction ID to assign
    next_txn_id: AtomicU32,
    
    /// Log manager reference
    log_manager: Arc<LogManager>,
    
    /// Active transactions map (txn_id -> Transaction)
    active_transactions: Mutex<HashMap<TxnId, Transaction>>,
}

impl TransactionManager {
    /// Create a new transaction manager
    pub fn new(log_manager: Arc<LogManager>) -> Self {
        Self {
            next_txn_id: AtomicU32::new(1), // Start from 1
            log_manager,
            active_transactions: Mutex::new(HashMap::new()),
        }
    }
    
    /// Begin a new transaction
    pub fn begin_transaction(&self, isolation_level: IsolationLevel) -> Result<TxnId, TransactionError> {
        // Generate a new transaction ID
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        
        // Create a new transaction
        let txn = Transaction::new(txn_id, self.log_manager.clone(), isolation_level);
        
        // Write the BEGIN log record
        txn.begin()?;
        
        // Store the transaction
        self.active_transactions.lock().unwrap().insert(txn_id, txn);
        
        Ok(txn_id)
    }
    
    /// Commit a transaction
    pub fn commit_transaction(&self, txn_id: TxnId) -> Result<(), TransactionError> {
        let mut txns = self.active_transactions.lock().unwrap();
        
        // Get the transaction
        let mut txn = match txns.remove(&txn_id) {
            Some(txn) => txn,
            None => return Err(TransactionError::InternalError(format!("Transaction {} not found", txn_id))),
        };
        
        // Commit the transaction
        txn.commit()?;
        
        Ok(())
    }
    
    /// Abort a transaction
    pub fn abort_transaction(&self, txn_id: TxnId) -> Result<(), TransactionError> {
        let mut txns = self.active_transactions.lock().unwrap();
        
        // Get the transaction
        let mut txn = match txns.remove(&txn_id) {
            Some(txn) => txn,
            None => return Err(TransactionError::InternalError(format!("Transaction {} not found", txn_id))),
        };
        
        // Abort the transaction
        txn.abort()?;
        
        Ok(())
    }
    
    /// Get a transaction by ID
    pub fn get_transaction(&self, txn_id: TxnId) -> Option<Transaction> {
        let txns = self.active_transactions.lock().unwrap();
        txns.get(&txn_id).cloned()
    }
    
    /// Check if a transaction exists
    pub fn transaction_exists(&self, txn_id: TxnId) -> bool {
        let txns = self.active_transactions.lock().unwrap();
        txns.contains_key(&txn_id)
    }
    
    /// Get all active transaction IDs
    pub fn get_active_transaction_ids(&self) -> Vec<TxnId> {
        let txns = self.active_transactions.lock().unwrap();
        txns.keys().cloned().collect()
    }
} 