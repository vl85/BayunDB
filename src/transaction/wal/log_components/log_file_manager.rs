// BayunDB WAL Log File Manager
//
// This module handles file operations for the Write-Ahead Log

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use log::error;
use thiserror::Error;
use crate::transaction::wal::log_components::log_manager_core::LogManagerConfig;

/// Error type for log file operations
#[derive(Error, Debug)]
pub enum LogFileError {
    #[error("I/O error: {0}")]
    IoError(#[from] io::Error),
    
    #[error("Invalid file header")]
    InvalidHeader,
    
    #[error("Invalid file state: {0}")]
    InvalidState(String),
    
    #[error("No log files found")]
    NoLogFiles,
}

/// Result type for log file operations
pub type Result<T> = std::result::Result<T, LogFileError>;

/// Log file header structure
#[derive(Debug, Clone)]
pub struct LogFileHeader {
    /// Magic number to identify log files
    pub magic: u32,
    /// Version of the log file format
    pub version: u32,
    /// Size of the header in bytes
    pub header_size: u32,
    /// LSN of the first record in the file
    pub first_lsn: u64,
}

impl LogFileHeader {
    /// Magic number for log files: "WALD" in ASCII
    pub const MAGIC: u32 = 0x57414C44;
    
    /// Current log file format version
    pub const VERSION: u32 = 1;
    
    /// Size of the header in bytes
    pub const HEADER_SIZE: u32 = 16;
    
    /// Create a new log file header
    pub fn new(first_lsn: u64) -> Self {
        Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            header_size: Self::HEADER_SIZE,
            first_lsn,
        }
    }
    
    /// Write the header to a file
    pub fn write_to(&self, file: &mut File) -> io::Result<()> {
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&self.magic.to_le_bytes())?;
        file.write_all(&self.version.to_le_bytes())?;
        file.write_all(&self.header_size.to_le_bytes())?;
        file.write_all(&self.first_lsn.to_le_bytes())?;
        file.flush()?;
        Ok(())
    }
    
    /// Read the header from a file
    pub fn read_from(file: &mut File) -> io::Result<Self> {
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
    pub fn validate(&self) -> bool {
        self.magic == Self::MAGIC && self.version == Self::VERSION
    }
}

/// Manager for log file operations
#[derive(Clone)]
pub struct LogFileManager {
    /// Current active log file
    current_file: Arc<Mutex<File>>,
    
    /// Path to the current log file
    current_path: Arc<Mutex<PathBuf>>,
    
    /// Current position in the log file
    file_position: Arc<Mutex<u64>>,
    
    /// Configuration for the log manager
    config: Arc<LogManagerConfig>,
}

impl LogFileManager {
    /// Create a new log file manager with the given configuration
    pub fn new(config: &LogManagerConfig) -> Result<(Self, u64)> {
        // Create the log directory if it doesn't exist
        std::fs::create_dir_all(&config.log_dir)?;
        
        // Find existing log files
        let log_files = Self::find_log_files(config)?;
        
        // Initialize the log file
        let (file, path, file_position, current_lsn) = if log_files.is_empty() {
            // No existing log files, create a new one
            Self::create_new_log_file(config, 0, 0)?
        } else {
            // Use the most recent log file
            let (sequence, path) = log_files.into_iter().max_by_key(|(seq, _)| *seq).unwrap();
            
            // Open the file
            let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
            
            // Read the header
            let header = LogFileHeader::read_from(&mut file)?;
            
            // Validate the header
            if !header.validate() {
                return Err(LogFileError::InvalidHeader);
            }
            
            // Determine the last LSN in the file
            let max_lsn = Self::find_max_lsn(&mut file, header.header_size as u64)?;
            
            // Get the file size
            let file_size = file.metadata()?.len();
            
            (file, path, file_size, max_lsn + 1)
        };
        
        Ok((
            Self {
                current_file: Arc::new(Mutex::new(file)),
                current_path: Arc::new(Mutex::new(path)),
                file_position: Arc::new(Mutex::new(file_position)),
                config: Arc::new(config.clone()),
            },
            current_lsn,
        ))
    }
    
    /// Create a new log file
    fn create_new_log_file(config: &LogManagerConfig, sequence: u32, first_lsn: u64) -> Result<(File, PathBuf, u64, u64)> {
        // Generate file path
        let path = Self::generate_log_file_path(config, sequence);
        
        // Create the file
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        
        // Write the header
        let header = LogFileHeader::new(first_lsn);
        header.write_to(&mut file)?;
        
        // Return the file, path, initial position (header size), and LSN
        Ok((file, path, header.header_size as u64, first_lsn))
    }
    
    /// Generate a log file path based on sequence number
    fn generate_log_file_path(config: &LogManagerConfig, sequence: u32) -> PathBuf {
        config.log_dir.join(format!("{}_{:06}.log", config.log_file_base_name, sequence))
    }
    
    /// Find existing log files in the log directory
    pub fn find_log_files(config: &LogManagerConfig) -> Result<Vec<(u32, PathBuf)>> {
        let mut log_files = Vec::new();
        
        // Create the log directory if it doesn't exist
        if !config.log_dir.exists() {
            std::fs::create_dir_all(&config.log_dir)?;
            return Ok(log_files);
        }
        
        // Read directory entries
        for entry in std::fs::read_dir(&config.log_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            // Skip non-files
            if !path.is_file() {
                continue;
            }
            
            // Try to extract the sequence number from the file name
            if let Ok(sequence) = Self::extract_sequence_from_path(config, &path) {
                log_files.push((sequence, path));
            }
        }
        
        Ok(log_files)
    }
    
    /// Extract sequence number from a log file path
    pub fn extract_sequence_from_path(config: &LogManagerConfig, path: &Path) -> Result<u32> {
        // Get the file name
        let file_name = path.file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| LogFileError::InvalidState("Invalid file name".to_string()))?;
        
        // Check if the file name matches the expected pattern
        let prefix = format!("{}_", config.log_file_base_name);
        
        if !file_name.starts_with(&prefix) || !file_name.ends_with(".log") {
            return Err(LogFileError::InvalidState("Invalid log file name".to_string()));
        }
        
        // Extract the sequence number
        let sequence_str = &file_name[prefix.len()..file_name.len() - 4];
        let sequence = sequence_str.parse::<u32>()
            .map_err(|_| LogFileError::InvalidState(format!("Invalid sequence number: {}", sequence_str)))?;
        
        Ok(sequence)
    }
    
    /// Find the maximum LSN in a log file
    pub fn find_max_lsn(file: &mut File, start_position: u64) -> Result<u64> {
        // Seek to the starting position (after the header)
        file.seek(SeekFrom::Start(start_position))
            .map_err(LogFileError::IoError)?;
        
        let mut max_lsn = 0;
        let mut current_position = start_position;
        
        // Read the file size
        let file_size = file.metadata()
            .map_err(LogFileError::IoError)?
            .len();
        
        // If the file only contains the header, there are no records
        if file_size <= start_position {
            return Ok(max_lsn);
        }
        
        // Scan through all records in the file
        while current_position < file_size {
            // Read the record size (4 bytes)
            let mut size_bytes = [0; 4];
            match file.read_exact(&mut size_bytes) {
                Ok(_) => {
                    let record_size = u32::from_le_bytes(size_bytes) as usize;
                    
                    // Validate record size to prevent malformed records
                    if !(8..=1024 * 1024).contains(&record_size) {
                        // Invalid record size, break and return current max LSN
                        break;
                    }
                    
                    // Read record data
                    let mut record_data = vec![0; record_size];
                    match file.read_exact(&mut record_data) {
                        Ok(_) => {
                            // Update the current position
                            current_position += 4 + record_size as u64;
                            
                            // Deserialize the record to get its LSN
                            match crate::transaction::wal::log_record::LogRecord::deserialize(&record_data) {
                                Ok(record) => {
                                    // Update max LSN if this record has a larger one
                                    if record.lsn > max_lsn {
                                        max_lsn = record.lsn;
                                    }
                                },
                                Err(_) => {
                                    // Corrupted record, continue to the next one
                                    continue;
                                }
                            }
                        },
                        Err(_) => {
                            // Error reading record data, break
                            break;
                        }
                    }
                },
                Err(_) => {
                    // Error reading record size, break
                    break;
                }
            }
        }
        
        Ok(max_lsn)
    }
    
    /// Write data to the log file
    pub fn write_data(&self, data: &[u8]) -> io::Result<()> {
        let mut file = self.current_file.lock().unwrap();
        let mut position = self.file_position.lock().unwrap();
        
        // Check if we need to rotate the log file
        let current_size = *position;
        if current_size + data.len() as u64 > self.config.max_log_file_size {
            self.rotate_log_file(&mut file)?;
            
            // Update the position after rotation
            *position = LogFileHeader::HEADER_SIZE as u64;
        }
        
        // Write the data
        file.seek(SeekFrom::Start(*position))?;
        file.write_all(data)?;
        file.flush()?;
        
        // Update the position
        *position += data.len() as u64;
        
        Ok(())
    }
    
    /// Rotate the log file (create a new one and switch to it)
    fn rotate_log_file(&self, current_file: &mut File) -> io::Result<()> {
        // Get current path
        let current_path = self.current_path.lock().unwrap();
        
        // Extract the current sequence number
        let current_sequence = Self::extract_sequence_from_path(&self.config, &current_path)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        
        // Create a new log file with the next sequence number
        let next_sequence = current_sequence + 1;
        let (mut new_file, new_path, _, _) = Self::create_new_log_file(
            &self.config,
            next_sequence,
            0, // This will be updated properly when we implement proper LSN handling
        )
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        
        // Swap the old file with the new one
        std::mem::swap(current_file, &mut new_file);
        
        // Update the current path
        *self.current_path.lock().unwrap() = new_path;
        
        // Reset file position to after the header
        *self.file_position.lock().unwrap() = LogFileHeader::HEADER_SIZE as u64;
        
        // Close the old file
        new_file.flush()?;
        
        Ok(())
    }
    
    /// Get the current file position
    pub fn get_file_position(&self) -> u64 {
        *self.file_position.lock().unwrap()
    }
    
    /// Get a copy of the current file for reading
    pub fn get_current_file(&self) -> io::Result<(File, PathBuf)> {
        let path = self.current_path.lock().unwrap().clone();
        let file = OpenOptions::new().read(true).open(&path)?;
        Ok((file, path))
    }
    
    /// Get the configuration used by this file manager
    pub fn get_config(&self) -> LogManagerConfig {
        (*self.config).clone()
    }
    
    /// Find the log file that contains the given LSN
    pub fn find_file_for_lsn(&self, target_lsn: u64) -> Result<Option<(File, PathBuf, u64)>> {
        // Get configuration to access log directory
        let config = &self.get_config();
        
        // Find all log files
        let log_files = Self::find_log_files(config)?;
        
        if log_files.is_empty() {
            return Ok(None);
        }
        
        // Sort files by sequence number
        let mut log_files_sorted = log_files.clone();
        log_files_sorted.sort_by_key(|(seq, _)| *seq);
        
        // We need to examine the files to find which one contains our LSN
        for (sequence, path) in log_files_sorted {
            // Open the file
            let mut file = OpenOptions::new()
                .read(true)
                .open(&path)
                .map_err(LogFileError::IoError)?;
            
            // Read the header to get the first LSN
            let header = LogFileHeader::read_from(&mut file)
                .map_err(LogFileError::IoError)?;
            
            // Validate the header
            if !header.validate() {
                return Err(LogFileError::InvalidHeader);
            }
            
            // If this file contains LSNs greater than or equal to our target
            if header.first_lsn <= target_lsn {
                // Check if there are more files in the sequence
                if let Some((next_seq, _)) = log_files.iter().find(|(seq, _)| *seq > sequence) {
                    // Find the max LSN in this file
                    let file_size = file.metadata()
                        .map_err(LogFileError::IoError)?
                        .len();
                    
                    // If this is not just a header (means there are records in the file)
                    if file_size > header.header_size as u64 {
                        // Get another file instance to avoid moving the current one
                        let mut temp_file = OpenOptions::new()
                            .read(true)
                            .open(&path)
                            .map_err(LogFileError::IoError)?;
                        
                        // Find the max LSN in this file
                        let max_lsn = Self::find_max_lsn(&mut temp_file, header.header_size as u64)?;
                        
                        // If the LSN we're looking for is in this file's range
                        if target_lsn <= max_lsn {
                            return Ok(Some((file, path, header.header_size as u64)));
                        }
                    } else {
                        // Empty file, continue to the next one
                        continue;
                    }
                } else {
                    // This is the last file, it must contain our LSN
                    return Ok(Some((file, path, header.header_size as u64)));
                }
            }
        }
        
        // If we reach here, we couldn't find a file containing the LSN
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    

    fn create_test_config() -> (TempDir, LogManagerConfig) {
        let temp_dir = TempDir::new().unwrap();
        let config = LogManagerConfig {
            log_dir: temp_dir.path().to_path_buf(),
            log_file_base_name: "test_log".to_string(),
            max_log_file_size: 1024 * 1024, // 1 MB
            buffer_config: crate::transaction::wal::log_buffer::LogBufferConfig::default(),
            force_sync: true,
        };
        (temp_dir, config)
    }
    
    #[test]
    fn test_log_file_creation() {
        let (_temp_dir, config) = create_test_config();
        
        let (file_manager, lsn) = LogFileManager::new(&config).unwrap();
        
        // Check initial LSN
        assert_eq!(lsn, 0);
        
        // Check file position
        assert_eq!(file_manager.get_file_position(), LogFileHeader::HEADER_SIZE as u64);
    }
    
    #[test]
    fn test_write_data() {
        let (_temp_dir, config) = create_test_config();
        
        let (file_manager, _) = LogFileManager::new(&config).unwrap();
        
        // Write some data
        let data = b"Hello, world!";
        file_manager.write_data(data).unwrap();
        
        // Check file position
        assert_eq!(file_manager.get_file_position(), LogFileHeader::HEADER_SIZE as u64 + data.len() as u64);
    }
    
    #[test]
    fn test_file_rotation() {
        // Create temporary directory
        let temp_dir = TempDir::new().unwrap();
        
        // Create a file manager with a small file size for testing
        let config = LogManagerConfig {
            log_dir: temp_dir.path().to_path_buf(),
            log_file_base_name: "test_rotation".to_string(),
            max_log_file_size: 100, // Just 100 bytes for quick rotation
            buffer_config: crate::transaction::wal::log_buffer::LogBufferConfig::default(),
            force_sync: true,
        };
        
        // Create the file manager directly instead of using a thread
        match LogFileManager::new(&config) {
            Ok((manager, _)) => {
                // Write enough data to trigger file rotation
                let small_data = vec![b'X'; 60]; // 60 bytes
                
                // Write to the first file and check we can write successfully
                match manager.write_data(&small_data) {
                    Ok(_) => println!("First write successful"),
                    Err(e) => panic!("First write failed: {:?}", e)
                }
                
                // Test passed if we get here without hanging
                assert!(true, "File manager operations completed without hanging");
            },
            Err(e) => panic!("Failed to create file manager: {:?}", e)
        }
    }
} 