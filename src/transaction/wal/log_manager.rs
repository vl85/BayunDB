use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}};
use thiserror::Error;

use crate::transaction::wal::log_buffer::{LogBuffer, LogBufferConfig, LogBufferError};
use crate::transaction::wal::log_record::{LogRecord, LogRecordType, LogRecordError, LogRecordContent};

/// Error type for log manager operations
#[derive(Error, Debug)]
pub enum LogManagerError {
    #[error("I/O error: {0}")]
    IoError(#[from] io::Error),
    
    #[error("Buffer error: {0}")]
    BufferError(#[from] LogBufferError),
    
    #[error("Log record error: {0}")]
    LogRecordError(#[from] LogRecordError),
    
    #[error("Invalid log file format")]
    InvalidFormat,
    
    #[error("Invalid log state: {0}")]
    InvalidState(String),
}

/// Result type for log manager operations
pub type Result<T> = std::result::Result<T, LogManagerError>;

/// Log file header structure
#[derive(Debug, Clone)]
struct LogFileHeader {
    /// Magic number to identify log files
    magic: u32,
    /// Version of the log file format
    version: u32,
    /// Size of the header in bytes
    header_size: u32,
    /// LSN of the first record in the file
    first_lsn: u64,
}

impl LogFileHeader {
    /// Magic number for log files: "WALD" in ASCII
    const MAGIC: u32 = 0x57414C44;
    
    /// Current log file format version
    const VERSION: u32 = 1;
    
    /// Size of the header in bytes
    const HEADER_SIZE: u32 = 16;
    
    /// Create a new log file header
    fn new(first_lsn: u64) -> Self {
        Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            header_size: Self::HEADER_SIZE,
            first_lsn,
        }
    }
    
    /// Write the header to a file
    fn write_to(&self, file: &mut File) -> io::Result<()> {
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&self.magic.to_le_bytes())?;
        file.write_all(&self.version.to_le_bytes())?;
        file.write_all(&self.header_size.to_le_bytes())?;
        file.write_all(&self.first_lsn.to_le_bytes())?;
        file.flush()?;
        Ok(())
    }
    
    /// Read the header from a file
    fn read_from(file: &mut File) -> io::Result<Self> {
        file.seek(SeekFrom::Start(0))?;
        
        let mut magic_bytes = [0; 4];
        file.read_exact(&mut magic_bytes)?;
        let magic = u32::from_le_bytes(magic_bytes);
        
        let mut version_bytes = [0; 4];
        file.read_exact(&mut version_bytes)?;
        let version = u32::from_le_bytes(version_bytes);
        
        let mut header_size_bytes = [0; 4];
        file.read_exact(&mut header_size_bytes)?;
        let header_size = u32::from_le_bytes(header_size_bytes);
        
        let mut first_lsn_bytes = [0; 8];
        file.read_exact(&mut first_lsn_bytes)?;
        let first_lsn = u64::from_le_bytes(first_lsn_bytes);
        
        Ok(Self {
            magic,
            version,
            header_size,
            first_lsn,
        })
    }
    
    /// Validate the header
    fn validate(&self) -> bool {
        self.magic == Self::MAGIC && self.version == Self::VERSION
    }
}

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
    pub buffer_config: LogBufferConfig,
    
    /// Whether to force sync on every commit
    pub force_sync: bool,
}

impl Default for LogManagerConfig {
    fn default() -> Self {
        Self {
            log_dir: PathBuf::from("logs"),
            log_file_base_name: "bayundb_log".to_string(),
            max_log_file_size: 100 * 1024 * 1024, // 100 MB
            buffer_config: LogBufferConfig::default(),
            force_sync: true,
        }
    }
}

/// Manager for write-ahead logging operations
pub struct LogManager {
    /// Configuration for the log manager
    config: LogManagerConfig,
    
    /// Current active log file
    current_log_file: Mutex<File>,
    
    /// Path to the current log file
    current_log_path: Mutex<PathBuf>,
    
    /// Current position in the log file
    file_position: AtomicU64,
    
    /// Current LSN to assign to new log records
    current_lsn: AtomicU64,
    
    /// In-memory buffer for log records
    log_buffer: LogBuffer,
    
    /// Flag indicating if the log manager is flushing
    is_flushing: Mutex<bool>,
}

impl LogManager {
    /// Create a new log manager with the given configuration
    pub fn new(config: LogManagerConfig) -> Result<Self> {
        // Create log directory if it doesn't exist
        std::fs::create_dir_all(&config.log_dir)?;
        
        // Find or create the current log file
        let (log_file, log_path, current_lsn, file_position) = Self::initialize_log_file(&config)?;
        
        // Create the log buffer
        let log_buffer = LogBuffer::new(config.buffer_config.clone());
        
        Ok(Self {
            config,
            current_log_file: Mutex::new(log_file),
            current_log_path: Mutex::new(log_path),
            file_position: AtomicU64::new(file_position),
            current_lsn: AtomicU64::new(current_lsn),
            log_buffer,
            is_flushing: Mutex::new(false),
        })
    }
    
    /// Create a new log manager with default configuration
    pub fn default() -> Result<Self> {
        Self::new(LogManagerConfig::default())
    }
    
    /// Initialize log file - either open existing or create new
    fn initialize_log_file(config: &LogManagerConfig) -> Result<(File, PathBuf, u64, u64)> {
        // Look for existing log files
        let mut log_files = Self::find_log_files(config)?;
        
        if log_files.is_empty() {
            // No existing log files, create a new one
            let path = Self::generate_log_file_path(config, 1);
            let mut file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(&path)?;
            
            // Write the header
            let header = LogFileHeader::new(1);
            header.write_to(&mut file)?;
            
            Ok((file, path, 1, header.header_size as u64))
        } else {
            // Open the most recent log file
            log_files.sort_by_key(|(sequence, _)| *sequence);
            let (sequence, path) = log_files.pop().unwrap();
            
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)?;
            
            // Read the header
            let header = LogFileHeader::read_from(&mut file)?;
            
            if !header.validate() {
                return Err(LogManagerError::InvalidFormat);
            }
            
            // Get the file size to determine position and next LSN
            let file_size = file.metadata()?.len();
            
            // If the file is at the maximum size, create a new one
            if file_size >= config.max_log_file_size {
                let new_path = Self::generate_log_file_path(config, sequence + 1);
                let mut new_file = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .open(&new_path)?;
                
                // Scan the old file to find the highest LSN
                let max_lsn = Self::find_max_lsn(&mut file, header.header_size as u64)?;
                
                // Write the header with the next LSN
                let new_header = LogFileHeader::new(max_lsn + 1);
                new_header.write_to(&mut new_file)?;
                
                Ok((new_file, new_path, max_lsn + 1, new_header.header_size as u64))
            } else {
                // Find the highest LSN in the file
                let max_lsn = Self::find_max_lsn(&mut file, header.header_size as u64)?;
                
                // Seek to the end of the file for appending
                file.seek(SeekFrom::End(0))?;
                
                Ok((file, path, max_lsn + 1, file_size))
            }
        }
    }
    
    /// Find the maximum LSN in a log file
    fn find_max_lsn(file: &mut File, start_position: u64) -> Result<u64> {
        let mut max_lsn = 0;
        let file_size = file.metadata()?.len();
        
        // Seek to the start of log records
        file.seek(SeekFrom::Start(start_position))?;
        
        // Read each log record and track the maximum LSN
        while file.stream_position()? < file_size {
            // Read record size
            let mut size_bytes = [0; 4];
            if file.read_exact(&mut size_bytes).is_err() {
                // End of file or corrupted record
                break;
            }
            
            let record_size = u32::from_le_bytes(size_bytes) as usize;
            
            // Read record data
            let mut record_data = vec![0; record_size];
            if file.read_exact(&mut record_data).is_err() {
                // End of file or corrupted record
                break;
            }
            
            // Parse the record
            match LogRecord::deserialize(&record_data) {
                Ok(record) => {
                    if record.lsn > max_lsn {
                        max_lsn = record.lsn;
                    }
                }
                Err(_) => {
                    // Corrupted record, ignore and continue
                    continue;
                }
            }
        }
        
        Ok(max_lsn)
    }
    
    /// Find all existing log files in the log directory
    fn find_log_files(config: &LogManagerConfig) -> Result<Vec<(u32, PathBuf)>> {
        let mut result = Vec::new();
        
        // Make sure the directory exists
        if !config.log_dir.exists() {
            return Ok(result);
        }
        
        // Scan the directory for log files
        for entry in std::fs::read_dir(&config.log_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            // Check if this is a log file
            if path.is_file() {
                if let Some(file_name) = path.file_name() {
                    if let Some(file_name_str) = file_name.to_str() {
                        // Parse the file name to extract the sequence number
                        let prefix = format!("{}_", config.log_file_base_name);
                        if file_name_str.starts_with(&prefix) {
                            if let Some(suffix) = file_name_str.strip_prefix(&prefix) {
                                if let Ok(sequence) = suffix.parse::<u32>() {
                                    result.push((sequence, path));
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Ok(result)
    }
    
    /// Generate a path for a new log file
    fn generate_log_file_path(config: &LogManagerConfig, sequence: u32) -> PathBuf {
        let file_name = format!("{}_{}", config.log_file_base_name, sequence);
        config.log_dir.join(file_name)
    }
    
    /// Append a log record to the log
    pub fn append_log_record(
        &self,
        txn_id: u32,
        prev_lsn: u64,
        record_type: LogRecordType,
        content: LogRecordContent,
    ) -> Result<u64> {
        // Get the next LSN
        let lsn = self.current_lsn.fetch_add(1, Ordering::SeqCst);
        
        // Create the log record
        let record = LogRecord::new(lsn, txn_id, prev_lsn, record_type, content);
        
        // Append to the buffer
        self.log_buffer.append(&record)?;
        
        // If this is a commit or abort and force_sync is enabled, flush immediately
        if self.config.force_sync && 
           (record_type == LogRecordType::Commit || record_type == LogRecordType::Abort) {
            self.flush()?;
        }
        
        Ok(lsn)
    }
    
    /// Flush the log buffer to disk
    pub fn flush(&self) -> Result<u64> {
        // Acquire the flushing lock
        let mut is_flushing = self.is_flushing.lock().map_err(|e| {
            LogManagerError::InvalidState(format!("Failed to acquire flushing lock: {}", e))
        })?;
        
        // If already flushing, return
        if *is_flushing {
            return Ok(0);
        }
        
        // Mark that we're flushing
        *is_flushing = true;
        
        // Create a writer closure to write to the log file
        let _file_pos = self.file_position.load(Ordering::SeqCst);
        let result = self.log_buffer.flush(|data| {
            // Get the log file
            let mut file = self.current_log_file.lock().map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("Failed to lock log file: {}", e))
            })?;
            
            // Check if we need to rotate the log file
            let current_size = file.metadata()?.len();
            if current_size + data.len() as u64 > self.config.max_log_file_size {
                // Create a new log file
                self.rotate_log_file(&mut file)?;
            }
            
            // Write the size of the buffer
            file.write_all(&(data.len() as u32).to_le_bytes())?;
            
            // Write the buffer contents
            file.write_all(data)?;
            
            // Update the file position
            self.file_position.store(
                file.stream_position()?,
                Ordering::SeqCst
            );
            
            // Sync to disk if force_sync is enabled
            if self.config.force_sync {
                file.sync_data()?;
            }
            
            Ok(())
        });
        
        // We're done flushing
        *is_flushing = false;
        
        result.map_err(|e| e.into())
    }
    
    /// Rotate to a new log file
    fn rotate_log_file(&self, file: &mut File) -> io::Result<()> {
        // Get the current log path
        let current_path = {
            let path_guard = self.current_log_path.lock().map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("Failed to lock log path: {}", e))
            })?;
            path_guard.clone()
        };
        
        // Extract the sequence number from the current path
        let file_name = current_path.file_name().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "Invalid log file path")
        })?.to_str().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "Invalid log file name")
        })?;
        
        let prefix = format!("{}_", self.config.log_file_base_name);
        let sequence = file_name.strip_prefix(&prefix).ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "Invalid log file name format")
        })?.parse::<u32>().map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Failed to parse sequence number: {}", e))
        })?;
        
        // Create a new log file
        let new_path = Self::generate_log_file_path(&self.config, sequence + 1);
        let mut new_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&new_path)?;
        
        // Write the header with the next LSN
        let next_lsn = self.current_lsn.load(Ordering::SeqCst);
        let header = LogFileHeader::new(next_lsn);
        header.write_to(&mut new_file)?;
        
        // Update the current log file and path
        {
            let mut path_guard = self.current_log_path.lock().map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("Failed to lock log path: {}", e))
            })?;
            *path_guard = new_path;
        }
        
        // Replace the file in the mutex
        *file = new_file;
        
        // Update the file position
        self.file_position.store(LogFileHeader::HEADER_SIZE as u64, Ordering::SeqCst);
        
        Ok(())
    }
    
    /// Flush the log up to the specified LSN
    pub fn flush_till_lsn(&self, _target_lsn: u64) -> Result<()> {
        self.flush()?;
        Ok(())
    }
    
    /// Get the current LSN
    pub fn current_lsn(&self) -> u64 {
        self.current_lsn.load(Ordering::SeqCst)
    }
    
    /// Run crash recovery
    pub fn recover(&self) -> Result<()> {
        // TODO: Implement recovery in Phase 3
        Ok(())
    }
    
    /// Create a checkpoint
    pub fn checkpoint(&self, active_txns: &[u32], dirty_pages: &[(u32, u64)]) -> Result<u64> {
        // Get the next LSN for the checkpoint
        let lsn = self.current_lsn.fetch_add(1, Ordering::SeqCst);
        
        // Create a checkpoint record
        let record = LogRecord::new_checkpoint(
            lsn,
            active_txns.to_vec(),
            dirty_pages.to_vec()
        );
        
        // Append to the buffer
        self.log_buffer.append(&record)?;
        
        // Force a flush
        self.flush()?;
        
        Ok(lsn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;
    
    // Helper to create a temp directory for testing
    fn create_test_log_dir() -> TempDir {
        TempDir::new().unwrap()
    }
    
    #[test]
    fn test_log_manager_creation() {
        let temp_dir = create_test_log_dir();
        
        let config = LogManagerConfig {
            log_dir: temp_dir.path().to_path_buf(),
            log_file_base_name: "test_log".to_string(),
            max_log_file_size: 1024 * 1024, // 1 MB
            buffer_config: LogBufferConfig::default(),
            force_sync: true,
        };
        
        let log_manager = LogManager::new(config).unwrap();
        
        // Verify the log directory was created
        assert!(temp_dir.path().exists());
        
        // Verify a log file was created
        let log_files = fs::read_dir(temp_dir.path()).unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.file_name().to_string_lossy().starts_with("test_log_")
            })
            .count();
        
        assert_eq!(log_files, 1);
    }
    
    #[test]
    fn test_log_record_append() {
        let temp_dir = create_test_log_dir();
        
        let config = LogManagerConfig {
            log_dir: temp_dir.path().to_path_buf(),
            log_file_base_name: "test_log".to_string(),
            max_log_file_size: 1024 * 1024, // 1 MB
            buffer_config: LogBufferConfig::default(),
            force_sync: false, // Don't force sync for testing
        };
        
        let log_manager = LogManager::new(config).unwrap();
        
        // Append a BEGIN record
        let lsn1 = log_manager.append_log_record(
            1, // txn_id
            0, // prev_lsn
            LogRecordType::Begin,
            LogRecordContent::Transaction(crate::transaction::wal::log_record::TransactionOperationContent {
                timestamp: 1234,
                metadata: None,
            }),
        ).unwrap();
        
        // Append a COMMIT record
        let lsn2 = log_manager.append_log_record(
            1, // txn_id
            lsn1, // prev_lsn
            LogRecordType::Commit,
            LogRecordContent::Transaction(crate::transaction::wal::log_record::TransactionOperationContent {
                timestamp: 5678,
                metadata: None,
            }),
        ).unwrap();
        
        // LSNs should be sequential
        assert_eq!(lsn2, lsn1 + 1);
        
        // Flush to ensure records are written to disk
        log_manager.flush().unwrap();
    }
    
    #[test]
    fn test_log_file_rotation() {
        let temp_dir = create_test_log_dir();
        
        // Create a config with a very small log file size to trigger rotation
        let config = LogManagerConfig {
            log_dir: temp_dir.path().to_path_buf(),
            log_file_base_name: "test_log".to_string(),
            max_log_file_size: 100, // Very small to trigger rotation
            buffer_config: LogBufferConfig {
                buffer_size: 64, // Small buffer for testing
                flush_threshold: 0.5,
            },
            force_sync: false, // Don't force sync for testing
        };
        
        let log_manager = LogManager::new(config.clone()).unwrap();
        
        // Append many records to trigger rotation
        for i in 0..20 {
            let lsn = log_manager.append_log_record(
                1, // txn_id
                i, // prev_lsn
                LogRecordType::Begin,
                LogRecordContent::Transaction(crate::transaction::wal::log_record::TransactionOperationContent {
                    timestamp: i as u64,
                    metadata: None,
                }),
            ).unwrap();
            
            // Force a flush every few records
            if i % 5 == 0 {
                log_manager.flush().unwrap();
            }
        }
        
        // Final flush
        log_manager.flush().unwrap();
        
        // Should have created multiple log files
        let log_files = fs::read_dir(temp_dir.path()).unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.file_name().to_string_lossy().starts_with("test_log_")
            })
            .count();
        
        assert!(log_files > 1);
    }
} 