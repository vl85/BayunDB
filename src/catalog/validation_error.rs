use thiserror::Error;

/// Validation errors that can occur during type checking
#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch {
        expected: String,
        actual: String,
    },
    #[error("NULL value not allowed for non-nullable column {0}")]
    NullValueNotAllowed(String),
    #[error("Invalid operation: {0} {1} {2}")]
    InvalidOperation(String, String, String),
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
    #[error("Table not found: {0}")]
    TableNotFound(String),
    #[error("Incompatible types for operation: {0} {1} {2}")]
    IncompatibleTypes(String, String, String),
    #[error("Value out of range for type: {0}")]
    ValueOutOfRange(String),
    #[error("Invalid date format: {0}")]
    InvalidDateFormat(String),
    #[error("Invalid timestamp format: {0}")]
    InvalidTimestampFormat(String),
    #[error("Missing value for not null column: {0}")]
    MissingValueForNotNullColumn(String),
}

/// Type validation result
pub type ValidationResult<T> = Result<T, ValidationError>; 