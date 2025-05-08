use thiserror::Error;
use crate::common::types::PageId;
use crate::storage::disk::DiskManagerError;

#[derive(Error, Debug)]
pub enum BufferPoolError {
    #[error("Page {0} not found in buffer pool")]
    PageNotFound(PageId),
    #[error("Buffer pool is full")]
    BufferPoolFull,
    #[error("Disk manager error: {0}")]
    DiskManagerError(#[from] DiskManagerError),
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
} 