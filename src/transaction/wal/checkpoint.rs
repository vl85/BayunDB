// BayunDB WAL Checkpoint Module
// This will be fully implemented in Phase 2

use std::sync::Arc;
use thiserror::Error;

use crate::transaction::wal::log_manager::{LogManager, LogManagerError};

/// Error type for checkpoint operations
#[derive(Error, Debug)]
pub enum CheckpointError {
    #[error("Log manager error: {0}")]
    LogManagerError(#[from] LogManagerError),
    
    #[error("Failed to checkpoint: {0}")]
    CheckpointError(String),
}

/// Result type for checkpoint operations
pub type Result<T> = std::result::Result<T, CheckpointError>;

/// Configuration for checkpoint behavior
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Minimum time between checkpoints in seconds
    pub checkpoint_interval: u64,
    
    /// Whether to perform fuzzy checkpoints
    pub fuzzy_checkpoint: bool,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            checkpoint_interval: 300,  // 5 minutes
            fuzzy_checkpoint: true,
        }
    }
}

/// Checkpoint manager responsible for creating periodic checkpoints
pub struct CheckpointManager {
    /// Log manager reference
    log_manager: Arc<LogManager>,
    
    /// Checkpoint configuration
    config: CheckpointConfig,
    
    /// Time of the last checkpoint
    last_checkpoint_time: std::time::Instant,
}

impl CheckpointManager {
    /// Create a new checkpoint manager
    pub fn new(log_manager: Arc<LogManager>, config: CheckpointConfig) -> Self {
        Self {
            log_manager,
            config,
            last_checkpoint_time: std::time::Instant::now(),
        }
    }
    
    /// Create a new checkpoint manager with default configuration
    pub fn default(log_manager: Arc<LogManager>) -> Self {
        Self::new(log_manager, CheckpointConfig::default())
    }
    
    /// Create a checkpoint
    pub fn create_checkpoint(&mut self) -> Result<u64> {
        // This is a stub implementation - Phase 2 will implement the actual checkpoint logic
        // For now, just create an empty checkpoint record
        
        let checkpoint_lsn = self.log_manager.checkpoint(&[], &[])?;
        
        // Update the last checkpoint time
        self.last_checkpoint_time = std::time::Instant::now();
        
        Ok(checkpoint_lsn)
    }
    
    /// Check if a checkpoint is needed based on the time interval
    pub fn checkpoint_needed(&self) -> bool {
        let elapsed = self.last_checkpoint_time.elapsed();
        elapsed.as_secs() >= self.config.checkpoint_interval
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::wal::log_manager::LogManagerConfig as WalLogManagerConfig; // Renamed to avoid conflict
    use tempfile::TempDir;
    use std::path::PathBuf;
    use std::thread;
    use std::time::Duration;

    fn get_test_log_manager() -> Arc<LogManager> {
        let temp_dir = TempDir::new().unwrap();
        let mut config = WalLogManagerConfig::default();
        config.log_dir = PathBuf::from(temp_dir.path());
        std::mem::forget(temp_dir);
        Arc::new(LogManager::new(config).unwrap())
    }

    #[test]
    fn test_checkpoint_config_default() {
        let config = CheckpointConfig::default();
        assert_eq!(config.checkpoint_interval, 300);
        assert_eq!(config.fuzzy_checkpoint, true);
    }

    #[test]
    fn test_checkpoint_manager_new() {
        let log_manager = get_test_log_manager();
        let config = CheckpointConfig { checkpoint_interval: 60, fuzzy_checkpoint: false };
        let cm = CheckpointManager::new(log_manager.clone(), config.clone());
        assert_eq!(cm.config.checkpoint_interval, 60);
        assert_eq!(cm.config.fuzzy_checkpoint, false);
        // last_checkpoint_time is set to now(), difficult to assert exact value

        let cm_default = CheckpointManager::default(log_manager);
        assert_eq!(cm_default.config.checkpoint_interval, CheckpointConfig::default().checkpoint_interval);
    }

    #[test]
    fn test_checkpoint_needed() {
        let log_manager = get_test_log_manager();
        let config = CheckpointConfig { checkpoint_interval: 1, fuzzy_checkpoint: true }; // 1 second interval
        let cm = CheckpointManager::new(log_manager, config);

        // Immediately after creation, checkpoint should not be needed (unless interval is 0)
        assert!(!cm.checkpoint_needed());

        // Wait for longer than the interval
        thread::sleep(Duration::from_secs(2));
        assert!(cm.checkpoint_needed());
    }

    #[test]
    fn test_create_checkpoint_stub() {
        let log_manager = get_test_log_manager();
        let mut cm = CheckpointManager::default(log_manager);
        let initial_time = cm.last_checkpoint_time;
        
        thread::sleep(Duration::from_millis(10)); // Ensure time advances a bit

        let result = cm.create_checkpoint();
        assert!(result.is_ok());
        let checkpoint_lsn = result.unwrap();
        assert!(checkpoint_lsn > 0); // LogManager should assign a new LSN
        assert!(cm.last_checkpoint_time > initial_time);
    }
} 