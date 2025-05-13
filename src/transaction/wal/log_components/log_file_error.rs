use thiserror::Error;
use std::io;

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

pub type Result<T> = std::result::Result<T, LogFileError>; 