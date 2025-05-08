use thiserror::Error;
use crate::storage::buffer::BufferPoolError;

#[derive(Error, Debug)]
pub enum BTreeError {
    #[error("Key not found")]
    KeyNotFound,
    
    #[error("Node too large for page")]
    NodeTooLarge,
    
    #[error("Serialization error: {0}")]
    SerializationError(String),
    
    #[error("Deserialization error: {0}")]
    DeserializationError(String),
    
    #[error("Invalid page format")]
    InvalidPageFormat,
    
    #[error("Buffer pool error: {0}")]
    BufferPoolError(#[from] BufferPoolError),
} 