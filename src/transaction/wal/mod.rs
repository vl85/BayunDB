// BayunDB Write-Ahead Logging Module

pub mod log_record;
pub mod log_buffer;
pub mod log_manager;
pub mod checkpoint;
pub mod recovery;
pub mod log_components;

// Re-export the core items for backward compatibility
pub use log_manager::LogManager;
pub use log_manager::LogManagerConfig;
pub use log_manager::Result;
pub use log_components::log_manager_core::LogManagerError;
pub use log_components::log_iterator::LogRecordIterator; 