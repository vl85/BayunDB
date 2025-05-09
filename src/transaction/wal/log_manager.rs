use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}};
use thiserror::Error;

use crate::transaction::wal::log_buffer::{LogBuffer, LogBufferConfig, LogBufferError};
use crate::transaction::wal::log_record::{LogRecord, LogRecordType, LogRecordError, LogRecordContent, DataOperationContent, TransactionOperationContent};

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

impl Clone for LogManager {
    fn clone(&self) -> Self {
        // Get the current values from the atomic fields
        let file_position = self.file_position.load(Ordering::SeqCst);
        let current_lsn = self.current_lsn.load(Ordering::SeqCst);
        
        // Clone the current log file
        let current_log_file = match self.current_log_file.lock() {
            Ok(file) => {
                // We need to duplicate the file handle
                let file_path = self.current_log_path.lock().unwrap().clone();
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&file_path)
                    .unwrap()
            },
            Err(_) => panic!("Failed to lock log file for cloning"),
        };
        
        // Clone the current log path
        let current_log_path = self.current_log_path.lock().unwrap().clone();
        
        Self {
            config: self.config.clone(),
            current_log_file: Mutex::new(current_log_file),
            current_log_path: Mutex::new(current_log_path),
            file_position: AtomicU64::new(file_position),
            current_lsn: AtomicU64::new(current_lsn),
            log_buffer: self.log_buffer.clone(),
            is_flushing: Mutex::new(*self.is_flushing.lock().unwrap()),
        }
    }
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
    
    /// Get an iterator over log records starting from the most recent checkpoint
    pub fn get_log_iterator_from_checkpoint(&self) -> Result<LogRecordIterator> {
        // Find the most recent checkpoint
        let checkpoint_lsn = self.find_last_checkpoint()?;
        
        // If no checkpoint found, start from beginning of log
        let start_lsn = if checkpoint_lsn > 0 { checkpoint_lsn } else { 1 };
        
        self.get_log_iterator_from_lsn(start_lsn)
    }
    
    /// Get an iterator over log records starting from a specific LSN
    pub fn get_log_iterator_from_lsn(&self, start_lsn: u64) -> Result<LogRecordIterator> {
        Ok(LogRecordIterator::new(Arc::new(self.clone()), start_lsn))
    }
    
    /// Get a specific log record by LSN
    pub fn get_log_record(&self, lsn: u64) -> Result<LogRecord> {
        // This could be optimized to directly seek to the position, but for now
        // we'll use the iterator to find the record
        let mut iter = self.get_log_iterator_from_lsn(lsn)?;
        
        // Find the specific record
        for record_result in &mut iter {
            let record = record_result?;
            if record.lsn == lsn {
                return Ok(record);
            }
            if record.lsn > lsn {
                // We've gone past the desired record
                return Err(LogManagerError::InvalidState(
                    format!("Log record with LSN {} not found", lsn)));
            }
        }
        
        Err(LogManagerError::InvalidState(
            format!("Log record with LSN {} not found", lsn)))
    }
    
    /// Append a compensation log record (CLR) for undo operations
    pub fn append_compensation_record(
        &self,
        txn_id: u32,
        undo_next_lsn: u64,
        op_type: LogRecordType,
        table_id: u32,
        page_id: u32,
        record_id: u32,
        before_image: Option<Vec<u8>>,
        after_image: Option<Vec<u8>>,
    ) -> Result<u64> {
        // Create appropriate content based on operation type
        let content = match op_type {
            LogRecordType::Update => {
                LogRecordContent::Data(DataOperationContent {
                    table_id,
                    page_id,
                    record_id,
                    before_image,
                    after_image,
                })
            },
            LogRecordType::Insert => {
                LogRecordContent::Data(DataOperationContent {
                    table_id,
                    page_id,
                    record_id,
                    before_image: None,
                    after_image,
                })
            },
            LogRecordType::Delete => {
                LogRecordContent::Data(DataOperationContent {
                    table_id,
                    page_id,
                    record_id,
                    before_image,
                    after_image: None,
                })
            },
            _ => {
                return Err(LogManagerError::InvalidState(
                    format!("Invalid operation type for CLR: {:?}", op_type)));
            }
        };
        
        // Append the log record
        // Note: CLRs are special because they point to the next record to undo (undo_next_lsn)
        // rather than the previous record of the same transaction
        self.append_log_record(txn_id, undo_next_lsn, op_type, content)
    }
    
    /// Append an abort record for a transaction
    pub fn append_abort_record(&self, txn_id: u32, prev_lsn: u64) -> Result<u64> {
        let abort_content = LogRecordContent::Transaction(TransactionOperationContent {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            metadata: None,
        });
        
        self.append_log_record(txn_id, prev_lsn, LogRecordType::Abort, abort_content)
    }
    
    /// Find the most recent checkpoint LSN
    fn find_last_checkpoint(&self) -> Result<u64> {
        // This is a simplified implementation
        // In a real system, we would maintain a separate checkpoint file
        
        // Get iterator from beginning of log
        let mut lsn = 0;
        let mut iter = self.get_log_iterator_from_lsn(1)?;
        
        // Find the last checkpoint record
        for record_result in &mut iter {
            let record = record_result?;
            if record.record_type == LogRecordType::Checkpoint {
                lsn = record.lsn;
            }
        }
        
        Ok(lsn)
    }
    
    /// Abort a transaction with the given ID and previous LSN
    pub fn abort_transaction(&self, txn_id: u32, prev_lsn: u64) -> Result<u64> {
        // Create an abort record
        let abort_content = LogRecordContent::Transaction(TransactionOperationContent {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            metadata: None,
        });
        
        // Append the abort record to the log
        self.append_log_record(txn_id, prev_lsn, LogRecordType::Abort, abort_content)
    }
    
    /// Get all log records for a specific transaction
    pub fn get_transaction_records(&self, txn_id: u32) -> Result<Vec<LogRecord>> {
        let mut records = Vec::new();
        
        // Create an iterator starting from the beginning (or last checkpoint)
        let iterator = self.get_log_iterator_from_checkpoint()?;
        
        // Collect all records for the specified transaction
        for record_result in iterator {
            match record_result {
                Ok(record) => {
                    if record.txn_id == txn_id {
                        records.push(record);
                    }
                },
                Err(e) => return Err(e),
            }
        }
        
        Ok(records)
    }
}

/// Iterator over log records
pub struct LogRecordIterator {
    log_manager: Arc<LogManager>,
    current_file: Option<File>,
    current_file_path: Option<PathBuf>,
    current_position: u64,
    current_lsn: u64,
    reached_end: bool,
}

impl LogRecordIterator {
    /// Create a new log record iterator
    fn new(log_manager: Arc<LogManager>, start_lsn: u64) -> Self {
        Self {
            log_manager,
            current_file: None,
            current_file_path: None,
            current_position: 0,
            current_lsn: start_lsn,
            reached_end: false,
        }
    }
    
    /// Find the file containing the given LSN
    fn find_file_for_lsn(&mut self, lsn: u64) -> Result<bool> {
        // Find all log files
        let config = &self.log_manager.config;
        let log_files = LogManager::find_log_files(config)?;
        
        if log_files.is_empty() {
            return Ok(false);
        }
        
        // Sort by sequence number (ascending)
        let mut sorted_files = log_files;
        sorted_files.sort_by_key(|(seq, _)| *seq);
        
        // Find the file containing the LSN
        let mut file_path = None;
        let mut file_header = None;
        
        for (_, path) in sorted_files {
            let mut file = OpenOptions::new()
                .read(true)
                .open(&path)?;
            
            // Read header
            let header = LogFileHeader::read_from(&mut file)?;
            
            // Check if this file contains the LSN
            if header.first_lsn <= lsn {
                file_path = Some(path);
                file_header = Some(header);
                self.current_file = Some(file);
                break;
            }
        }
        
        if let (Some(path), Some(header)) = (file_path, file_header) {
            self.current_file_path = Some(path);
            
            // Position at the start of records section
            self.current_position = LogFileHeader::HEADER_SIZE as u64;
            
            // If this is the first call, we need to find the right position for our start_lsn
            if lsn > header.first_lsn && self.current_position == LogFileHeader::HEADER_SIZE as u64 {
                // Scan forward to find the record with the desired LSN
                self.seek_to_lsn(lsn)?;
            }
            
            Ok(true)
        } else {
            Ok(false)
        }
    }
    
    /// Seek to the position of a specific LSN
    fn seek_to_lsn(&mut self, target_lsn: u64) -> Result<()> {
        if let Some(file) = &mut self.current_file {
            // Start from the beginning of records section
            let mut position = LogFileHeader::HEADER_SIZE as u64;
            file.seek(SeekFrom::Start(position))?;
            
            // Read records until we find one with LSN >= target_lsn
            loop {
                // Remember position before reading
                let record_position = position;
                
                // Read record size
                let mut size_bytes = [0; 4];
                if file.read_exact(&mut size_bytes).is_err() {
                    // End of file
                    break;
                }
                let record_size = u32::from_le_bytes(size_bytes);
                
                // Read record data
                let mut record_data = vec![0; record_size as usize];
                if file.read_exact(&mut record_data).is_err() {
                    // End of file or corrupted record
                    break;
                }
                
                // Deserialize to check LSN
                match LogRecord::deserialize(&record_data) {
                    Ok(record) => {
                        if record.lsn >= target_lsn {
                            // Found it, position file pointer back to start of this record
                            file.seek(SeekFrom::Start(record_position))?;
                            self.current_position = record_position;
                            return Ok(());
                        }
                    },
                    Err(_) => {
                        // Corrupted record, just continue
                    }
                }
                
                // Move to next record
                position += 4 + record_size as u64;
                file.seek(SeekFrom::Start(position))?;
            }
            
            // If we didn't find it, position at the end of file
            self.current_position = file.seek(SeekFrom::End(0))?;
        }
        
        Ok(())
    }
    
    /// Move to the next log file
    fn move_to_next_file(&mut self) -> Result<bool> {
        if let Some(current_path) = &self.current_file_path {
            // Extract sequence number from current file
            let config = &self.log_manager.config;
            let current_seq = LogManager::extract_sequence_from_path(config, current_path)?;
            
            // Look for the next file in sequence
            let next_seq = current_seq + 1;
            let next_path = LogManager::generate_log_file_path(config, next_seq);
            
            if next_path.exists() {
                // Open the next file
                let file = OpenOptions::new()
                    .read(true)
                    .open(&next_path)?;
                
                self.current_file = Some(file);
                self.current_file_path = Some(next_path);
                self.current_position = LogFileHeader::HEADER_SIZE as u64;
                
                return Ok(true);
            }
        }
        
        Ok(false)
    }
}

impl Iterator for LogRecordIterator {
    type Item = Result<LogRecord>;
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.reached_end {
            return None;
        }
        
        // If we don't have a file open yet, find the right one
        if self.current_file.is_none() {
            match self.find_file_for_lsn(self.current_lsn) {
                Ok(found) => {
                    if !found {
                        self.reached_end = true;
                        return None;
                    }
                },
                Err(err) => {
                    self.reached_end = true;
                    return Some(Err(err));
                }
            }
        }
        
        // Read the next record
        if let Some(file) = &mut self.current_file {
            // Seek to current position
            if let Err(err) = file.seek(SeekFrom::Start(self.current_position)) {
                self.reached_end = true;
                return Some(Err(LogManagerError::IoError(err)));
            }
            
            // Read record size
            let mut size_bytes = [0; 4];
            match file.read_exact(&mut size_bytes) {
                Ok(_) => {
                    let record_size = u32::from_le_bytes(size_bytes);
                    
                    // Read record data
                    let mut record_data = vec![0; record_size as usize];
                    match file.read_exact(&mut record_data) {
                        Ok(_) => {
                            // Update position for next read
                            self.current_position += 4 + record_size as u64;
                            
                            // Return deserialized record
                            match LogRecord::deserialize(&record_data) {
                                Ok(record) => {
                                    return Some(Ok(record));
                                },
                                Err(err) => {
                                    self.reached_end = true;
                                    return Some(Err(LogManagerError::LogRecordError(err)));
                                }
                            }
                        },
                        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                            // End of file, try next file
                            match self.move_to_next_file() {
                                Ok(found_next) => {
                                    if !found_next {
                                        self.reached_end = true;
                                        return None;
                                    }
                                    // Continue with the next file
                                    return self.next();
                                },
                                Err(err) => {
                                    self.reached_end = true;
                                    return Some(Err(err));
                                }
                            }
                        },
                        Err(err) => {
                            self.reached_end = true;
                            return Some(Err(LogManagerError::IoError(err)));
                        }
                    }
                },
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                    // End of file, try next file
                    match self.move_to_next_file() {
                        Ok(found_next) => {
                            if !found_next {
                                self.reached_end = true;
                                return None;
                            }
                            // Continue with the next file
                            return self.next();
                        },
                        Err(err) => {
                            self.reached_end = true;
                            return Some(Err(err));
                        }
                    }
                },
                Err(err) => {
                    self.reached_end = true;
                    return Some(Err(LogManagerError::IoError(err)));
                }
            }
        }
        
        self.reached_end = true;
        None
    }
}

// Helper method to extract sequence number from log file path
impl LogManager {
    fn extract_sequence_from_path(config: &LogManagerConfig, path: &Path) -> Result<u32> {
        // Example: extract "001" from "bayundb_log_001.log"
        let file_name = path.file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| LogManagerError::InvalidState("Invalid log file path".to_string()))?;
        
        let base_name = &config.log_file_base_name;
        
        // Check if the file name starts with the base name
        if !file_name.starts_with(base_name) {
            return Err(LogManagerError::InvalidState(
                format!("Log file {} does not match base name {}", file_name, base_name)));
        }
        
        // Extract the sequence number part
        let seq_part = file_name
            .strip_prefix(&format!("{}_", base_name))
            .and_then(|s| s.strip_suffix(".log"))
            .ok_or_else(|| LogManagerError::InvalidState(
                format!("Invalid log file name format: {}", file_name)))?;
        
        // Parse the sequence number
        seq_part.parse::<u32>()
            .map_err(|_| LogManagerError::InvalidState(
                format!("Invalid sequence number in log file: {}", file_name)))
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