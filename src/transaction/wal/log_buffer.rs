use std::sync::Mutex;
use thiserror::Error;
use parking_lot::RwLock;
use crate::transaction::wal::log_record::{LogRecord, LogRecordError};

/// Error type for log buffer operations
#[derive(Error, Debug)]
pub enum LogBufferError {
    #[error("Buffer is full")]
    BufferFull,
    
    #[error("Failed to acquire buffer lock: {0}")]
    LockError(String),
    
    #[error("Log record error: {0}")]
    LogRecordError(#[from] LogRecordError),
    
    #[error("Invalid buffer state: {0}")]
    InvalidState(String),
}

/// Result type for log buffer operations
pub type Result<T> = std::result::Result<T, LogBufferError>;

/// Configuration for log buffer behavior
#[derive(Debug, Clone)]
pub struct LogBufferConfig {
    /// Size of the buffer in bytes
    pub buffer_size: usize,
    
    /// Flush threshold as a percentage of buffer capacity
    pub flush_threshold: f32,
}

impl Default for LogBufferConfig {
    fn default() -> Self {
        Self {
            buffer_size: 1024 * 1024, // 1 MB default
            flush_threshold: 0.75,    // 75% default
        }
    }
}

/// A buffer segment containing serialized log records
#[derive(Debug)]
struct BufferSegment {
    /// The actual buffer containing serialized log records
    data: Vec<u8>,
    
    /// Current position in the buffer
    pos: usize,
    
    /// Maximum LSN in this segment
    max_lsn: u64,
}

impl BufferSegment {
    /// Create a new buffer segment with the specified capacity
    fn new(capacity: usize) -> Self {
        Self {
            data: vec![0; capacity],
            pos: 0,
            max_lsn: 0,
        }
    }
    
    /// Check if the segment has enough space for the given data
    fn has_space(&self, data_len: usize) -> bool {
        self.pos + data_len <= self.data.len()
    }
    
    /// Append serialized log record to the segment
    fn append(&mut self, record: &LogRecord, data: &[u8]) -> Result<()> {
        if !self.has_space(data.len()) {
            return Err(LogBufferError::BufferFull);
        }
        
        // Copy the data into the buffer
        self.data[self.pos..self.pos + data.len()].copy_from_slice(data);
        self.pos += data.len();
        
        // Update max LSN
        if record.lsn > self.max_lsn {
            self.max_lsn = record.lsn;
        }
        
        Ok(())
    }
    
    /// Reset the segment for reuse
    fn reset(&mut self) {
        self.pos = 0;
        self.max_lsn = 0;
    }
    
    /// Get the content of the buffer up to the current position
    fn content(&self) -> &[u8] {
        &self.data[0..self.pos]
    }
    
    /// Check if the segment is empty
    fn is_empty(&self) -> bool {
        self.pos == 0
    }
}

/// Log buffer that temporarily holds log records in memory before they are written to disk
#[derive(Debug)]
pub struct LogBuffer {
    /// Active buffer where new records are appended
    active_buffer: RwLock<BufferSegment>,
    
    /// Flush buffer that is being written to disk
    flush_buffer: RwLock<BufferSegment>,
    
    /// Configuration for the buffer
    config: LogBufferConfig,
    
    /// Flag indicating a flush is in progress
    flushing: Mutex<bool>,
}

impl Clone for LogBuffer {
    fn clone(&self) -> Self {
        // Clone the active buffer
        let active = self.active_buffer.read();
        let active_clone = BufferSegment {
            data: active.data.clone(),
            pos: active.pos,
            max_lsn: active.max_lsn,
        };
        
        // Clone the flush buffer
        let flush = self.flush_buffer.read();
        let flush_clone = BufferSegment {
            data: flush.data.clone(),
            pos: flush.pos,
            max_lsn: flush.max_lsn,
        };
        
        // Clone the is_flushing flag
        let is_flushing = match self.flushing.lock() {
            Ok(guard) => *guard,
            Err(_) => false, // Default to false if we can't get the lock
        };
        
        Self {
            active_buffer: RwLock::new(active_clone),
            flush_buffer: RwLock::new(flush_clone),
            config: self.config.clone(),
            flushing: Mutex::new(is_flushing),
        }
    }
}

impl LogBuffer {
    /// Create a new log buffer with the given configuration
    pub fn new(config: LogBufferConfig) -> Self {
        Self {
            active_buffer: RwLock::new(BufferSegment::new(config.buffer_size)),
            flush_buffer: RwLock::new(BufferSegment::new(config.buffer_size)),
            config,
            flushing: Mutex::new(false),
        }
    }
    
    /// Create a new log buffer with default configuration
    pub fn default() -> Self {
        Self::new(LogBufferConfig::default())
    }
    
    /// Append a log record to the buffer
    pub fn append(&self, record: &LogRecord) -> Result<()> {
        // Serialize the record
        let data = record.serialize()?;
        
        // Get the active buffer for writing
        let mut buffer = self.active_buffer.write();
        
        // Check if we have space in the active buffer
        if !buffer.has_space(data.len()) {
            // We need to flush before we can append
            // Release the lock and request a flush
            drop(buffer);
            self.swap_buffers()?;
            
            // Re-acquire the active buffer
            buffer = self.active_buffer.write();
            
            // If we still don't have space, the record is too large
            if !buffer.has_space(data.len()) {
                return Err(LogBufferError::BufferFull);
            }
        }
        
        // Append the record to the active buffer
        buffer.append(record, &data)?;
        
        // Check if we need to flush based on the threshold
        let flush_size = (self.config.buffer_size as f32 * self.config.flush_threshold) as usize;
        if buffer.pos >= flush_size {
            // Release the lock and swap buffers
            drop(buffer);
            self.swap_buffers()?;
        }
        
        Ok(())
    }
    
    /// Swap active and flush buffers
    fn swap_buffers(&self) -> Result<()> {
        // Acquire flushing lock
        let mut is_flushing = self.flushing.lock().map_err(|e| {
            LogBufferError::LockError(format!("Failed to acquire flushing lock: {}", e))
        })?;
        
        // If a flush is already in progress, we can just return
        if *is_flushing {
            return Ok(());
        }
        
        // Mark that we're flushing
        *is_flushing = true;
        
        // Acquire both buffer locks
        let mut active = self.active_buffer.write();
        let mut flush = self.flush_buffer.write();
        
        // If active buffer is empty, no need to swap
        if active.is_empty() {
            *is_flushing = false;
            return Ok(());
        }
        
        // Swap buffers - the current active buffer becomes the flush buffer,
        // and the flush buffer becomes the new active buffer
        std::mem::swap(&mut *active, &mut *flush);
        
        // Reset the now-active buffer for new writes
        active.reset();
        
        // Release locks
        drop(active);
        drop(flush);
        
        // We're no longer flushing
        *is_flushing = false;
        
        Ok(())
    }
    
    /// Flush the buffer to the provided writer function
    pub fn flush<F>(&self, mut writer: F) -> Result<u64>
    where
        F: FnMut(&[u8]) -> std::io::Result<()>
    {
        // Swap buffers if active buffer has data
        self.swap_buffers()?;
        
        // Get the flush buffer for reading
        let mut flush_buffer = self.flush_buffer.write();
        
        // If flush buffer is empty, nothing to do
        if flush_buffer.is_empty() {
            return Ok(0);
        }
        
        // Get the maximum LSN in this buffer
        let max_lsn = flush_buffer.max_lsn;
        
        // Write the buffer contents to disk
        writer(flush_buffer.content()).map_err(|e| {
            LogBufferError::InvalidState(format!("Failed to write buffer to disk: {}", e))
        })?;
        
        // Reset the flush buffer after successful write
        flush_buffer.reset();
        
        // Return the maximum LSN that was flushed
        Ok(max_lsn)
    }
    
    /// Force an immediate flush of all buffers
    pub fn force_flush<F>(&self, writer: F) -> Result<u64>
    where
        F: FnMut(&[u8]) -> std::io::Result<()>
    {
        // Swap buffers to ensure all data is moved to flush buffer
        self.swap_buffers()?;
        
        // Flush the data
        self.flush(writer)
    }
    
    /// Get current buffer utilization as a percentage
    pub fn utilization(&self) -> f32 {
        let active = self.active_buffer.read();
        active.pos as f32 / self.config.buffer_size as f32
    }
    
    /// Check if the buffer is currently empty
    pub fn is_empty(&self) -> bool {
        let active = self.active_buffer.read();
        let flush = self.flush_buffer.read();
        
        // The buffer is empty only if both segments are empty
        active.is_empty() && flush.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    fn create_test_record(lsn: u64, txn_id: u32) -> LogRecord {
        LogRecord::new_begin(lsn, txn_id)
    }
    
    #[test]
    fn test_buffer_append() {
        let config = LogBufferConfig {
            buffer_size: 1024, // Small buffer for testing
            flush_threshold: 0.75,
        };
        
        let buffer = LogBuffer::new(config);
        
        // Append a record
        let record = create_test_record(1, 1);
        buffer.append(&record).unwrap();
        
        // Check that the buffer is not empty
        assert!(!buffer.is_empty());
    }
    
    #[test]
    fn test_buffer_flush() {
        let config = LogBufferConfig {
            buffer_size: 1024, // Small buffer for testing
            flush_threshold: 0.75,
        };
        
        let buffer = LogBuffer::new(config);
        
        // Append a record
        let record = create_test_record(1, 1);
        buffer.append(&record).unwrap();
        
        // Create a writer that just collects the data
        let mut output = Vec::new();
        let max_lsn = buffer.force_flush(|data| {
            output.extend_from_slice(data);
            Ok(())
        }).unwrap();
        
        // Check that we wrote some data
        assert!(!output.is_empty());
        
        // Check that the max LSN is correct
        assert_eq!(max_lsn, 1);
        
        // Check that the buffer is now empty
        assert!(buffer.is_empty());
    }
    
    #[test]
    fn test_buffer_swap() {
        let config = LogBufferConfig {
            buffer_size: 1024, // Small buffer for testing
            flush_threshold: 0.75,
        };
        
        let buffer = LogBuffer::new(config);
        
        // Append records until we hit the threshold and trigger a swap
        let mut lsn = 0;
        loop {
            lsn += 1;
            let record = create_test_record(lsn, 1);
            buffer.append(&record).unwrap();
            
            // If utilization is low after append, we must have swapped
            if buffer.utilization() < 0.1 {
                break;
            }
            
            // Safety valve
            if lsn > 1000 {
                panic!("Buffer didn't swap after 1000 records");
            }
        }
        
        // Create a writer that just collects the data
        let mut output = Vec::new();
        let max_lsn = buffer.flush(|data| {
            output.extend_from_slice(data);
            Ok(())
        }).unwrap();
        
        // Check that we wrote some data
        assert!(!output.is_empty());
        
        // Check that we have some LSN value (we don't know exactly what it will be)
        assert!(max_lsn > 0);
    }
} 