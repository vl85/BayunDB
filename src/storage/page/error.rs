use thiserror::Error;

#[derive(Error, Debug)]
pub enum PageError {
    #[error("Not enough space in page")]
    InsufficientSpace,
    #[error("Record not found")]
    RecordNotFound,
    #[error("Invalid record ID")]
    InvalidRecordId,
    #[error("Record already exists at this ID")]
    DuplicateRecord,
} 