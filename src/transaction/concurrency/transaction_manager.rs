use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU32, Ordering};

use crate::common::types::TxnId;
use crate::transaction::{Transaction, IsolationLevel, TransactionError};
use crate::transaction::wal::log_manager::LogManager;
use crate::transaction::TransactionState;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::wal::log_manager::{LogManager, LogManagerConfig};
    use crate::transaction::TransactionState;
    use tempfile::TempDir;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn get_test_log_manager() -> Arc<LogManager> {
        let temp_dir = TempDir::new().unwrap();
        let mut config = LogManagerConfig::default();
        config.log_dir = PathBuf::from(temp_dir.path());
        std::mem::forget(temp_dir); // Leak to keep dir alive for test
        Arc::new(LogManager::new(config).unwrap())
    }

    #[test]
    fn test_tm_new() {
        let log_manager = get_test_log_manager();
        let tm = TransactionManager::new(log_manager);
        assert_eq!(tm.next_txn_id.load(Ordering::SeqCst), 1);
        assert!(tm.active_transactions.lock().unwrap().is_empty());
    }

    #[test]
    fn test_tm_begin_transaction() {
        let log_manager = get_test_log_manager();
        let tm = TransactionManager::new(log_manager);

        let txn_id_1_result = tm.begin_transaction(IsolationLevel::ReadCommitted);
        assert!(txn_id_1_result.is_ok());
        let txn_id_1 = txn_id_1_result.unwrap();
        assert_eq!(txn_id_1, 1);
        assert_eq!(tm.next_txn_id.load(Ordering::SeqCst), 2);
        assert!(tm.transaction_exists(txn_id_1));
        
        let txn = tm.get_transaction(txn_id_1).unwrap();
        assert_eq!(txn.id(), txn_id_1);
        assert_eq!(txn.state(), TransactionState::Active);
        assert!(txn.first_lsn() > 0); // Begin should have written to log

        let txn_id_2_result = tm.begin_transaction(IsolationLevel::Serializable);
        assert!(txn_id_2_result.is_ok());
        let txn_id_2 = txn_id_2_result.unwrap();
        assert_eq!(txn_id_2, 2);
        assert_eq!(tm.next_txn_id.load(Ordering::SeqCst), 3);
        assert!(tm.transaction_exists(txn_id_2));
    }

    #[test]
    fn test_tm_commit_transaction() {
        let log_manager = get_test_log_manager();
        let tm = TransactionManager::new(log_manager);

        let txn_id = tm.begin_transaction(IsolationLevel::ReadCommitted).unwrap();
        assert!(tm.transaction_exists(txn_id));

        let commit_result = tm.commit_transaction(txn_id);
        assert!(commit_result.is_ok());
        assert!(!tm.transaction_exists(txn_id));

        // Try to commit non-existent txn
        let commit_again_result = tm.commit_transaction(txn_id);
        assert!(commit_again_result.is_err());
        if let Err(TransactionError::InternalError(_)) = commit_again_result {
            // Expected
        } else {
            panic!("Expected InternalError for committing non-existent txn");
        }

        // Try to commit a txn ID that was never begun
         let commit_never_begun_result = tm.commit_transaction(999);
         assert!(commit_never_begun_result.is_err());
    }

    #[test]
    fn test_tm_abort_transaction() {
        let log_manager = get_test_log_manager();
        let tm = TransactionManager::new(log_manager);

        let txn_id = tm.begin_transaction(IsolationLevel::ReadCommitted).unwrap();
        assert!(tm.transaction_exists(txn_id));

        let abort_result = tm.abort_transaction(txn_id);
        assert!(abort_result.is_ok());
        assert!(!tm.transaction_exists(txn_id));

        // Try to abort non-existent txn
        let abort_again_result = tm.abort_transaction(txn_id);
        assert!(abort_again_result.is_err());
        if let Err(TransactionError::InternalError(_)) = abort_again_result {
            // Expected
        } else {
            panic!("Expected InternalError for aborting non-existent txn");
        }
    }

    #[test]
    fn test_tm_get_active_transactions() {
        let log_manager = get_test_log_manager();
        let tm = TransactionManager::new(log_manager);

        assert!(tm.get_active_transaction_ids().is_empty());

        let txn_id_1 = tm.begin_transaction(IsolationLevel::ReadCommitted).unwrap();
        let active_ids_1 = tm.get_active_transaction_ids();
        assert_eq!(active_ids_1.len(), 1);
        assert!(active_ids_1.contains(&txn_id_1));

        let txn_id_2 = tm.begin_transaction(IsolationLevel::Serializable).unwrap();
        let active_ids_2 = tm.get_active_transaction_ids();
        assert_eq!(active_ids_2.len(), 2);
        assert!(active_ids_2.contains(&txn_id_1));
        assert!(active_ids_2.contains(&txn_id_2));

        tm.commit_transaction(txn_id_1).unwrap();
        let active_ids_3 = tm.get_active_transaction_ids();
        assert_eq!(active_ids_3.len(), 1);
        assert!(active_ids_3.contains(&txn_id_2));

        tm.abort_transaction(txn_id_2).unwrap();
        assert!(tm.get_active_transaction_ids().is_empty());
    }
} 