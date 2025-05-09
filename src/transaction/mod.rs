// BayunDB Transaction Management Module

pub mod wal;
pub mod concurrency;
pub mod recovery;

// Public exports
pub use wal::log_manager::LogManager;
pub use wal::log_record::{LogRecord, LogRecordType};
pub use concurrency::{Transaction, TransactionState, IsolationLevel, TransactionError};
pub use concurrency::transaction_manager::TransactionManager;

// Re-export transaction manager when it's implemented
// pub use transaction_manager::TransactionManager; 