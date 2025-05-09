use std::fmt;
use thiserror::Error;
use crate::common::types::PageId;
use crate::storage::disk::DiskManagerError;
use crate::transaction::TransactionError;
use crate::transaction::wal::log_manager::LogManagerError;

#[derive(Error, Debug)]
pub enum BufferPoolError {
    #[error("Page {0} not found")]
    PageNotFound(PageId),
    
    #[error("Page {0} is pinned")]
    PagePinned(PageId),

    #[error("No free frames available")]
    NoFreeFrames,
    
    #[error("Buffer pool is full")]
    BufferPoolFull,
    
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("Disk manager error: {0}")]
    DiskManagerError(#[from] DiskManagerError),
    
    #[error("Transaction error: {0}")]
    TransactionError(#[from] TransactionError),
    
    #[error("WAL error: {0}")]
    WalError(#[from] LogManagerError),
} 