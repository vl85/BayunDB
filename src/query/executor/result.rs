// Query Result Implementation
//
// This module defines the result types for query execution.

use std::collections::HashMap;
use std::fmt;
use std::cmp::{Ordering, Eq};
use std::hash::{Hash, Hasher};
use serde;
use thiserror::Error;
use hex;
use linked_hash_map::LinkedHashMap;

use crate::query::parser::components::ParseError;
use crate::storage::page::PageError;
use crate::storage::buffer::BufferPoolError;

/// Possible data types for values in a row
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DataValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Boolean(bool),
    Date(String),
    Timestamp(String),
    Blob(Vec<u8>),
}

impl Eq for DataValue {}

impl Hash for DataValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            DataValue::Null => 0.hash(state),
            DataValue::Integer(i) => { 1.hash(state); i.hash(state); }
            DataValue::Float(f) => { 2.hash(state); f.to_bits().hash(state); }
            DataValue::Text(s) => { 3.hash(state); s.hash(state); }
            DataValue::Boolean(b) => { 4.hash(state); b.hash(state); }
            DataValue::Date(s) => { 5.hash(state); s.hash(state); }
            DataValue::Timestamp(s) => { 6.hash(state); s.hash(state); }
            DataValue::Blob(b) => { 7.hash(state); b.hash(state); }
        }
    }
}

impl fmt::Display for DataValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataValue::Null => write!(f, "NULL"),
            DataValue::Integer(i) => write!(f, "{}", i),
            DataValue::Float(fl) => write!(f, "{}", fl),
            DataValue::Text(s) => write!(f, "\"{}\"", s),
            DataValue::Boolean(b) => write!(f, "{}", b),
            DataValue::Date(s) => write!(f, "DATE '{}'", s),
            DataValue::Timestamp(s) => write!(f, "TIMESTAMP '{}'", s),
            DataValue::Blob(b) => write!(f, "BLOB ({} bytes)", b.len()),
        }
    }
}

impl PartialOrd for DataValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (DataValue::Null, DataValue::Null) => Some(Ordering::Equal),
            (DataValue::Null, _) => Some(Ordering::Less),
            (_, DataValue::Null) => Some(Ordering::Greater),
            
            (DataValue::Integer(a), DataValue::Integer(b)) => a.partial_cmp(b),
            (DataValue::Float(a), DataValue::Float(b)) => a.partial_cmp(b),
            (DataValue::Integer(a), DataValue::Float(b)) => (*a as f64).partial_cmp(b),
            (DataValue::Float(a), DataValue::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (DataValue::Text(a), DataValue::Text(b)) => Some(a.cmp(b)),
            (DataValue::Boolean(a), DataValue::Boolean(b)) => a.partial_cmp(b),
            (DataValue::Date(a), DataValue::Date(b)) => Some(a.cmp(b)), 
            (DataValue::Timestamp(a), DataValue::Timestamp(b)) => Some(a.cmp(b)),
            (DataValue::Blob(_), DataValue::Blob(_)) => None, // Blobs are typically not ordered beyond equality

            // Comparing Text with Date/Timestamp (assuming string representations)
            (DataValue::Text(a), DataValue::Date(b)) => Some(a.cmp(b)),
            (DataValue::Date(a), DataValue::Text(b)) => Some(a.cmp(b)),
            (DataValue::Text(a), DataValue::Timestamp(b)) => Some(a.cmp(b)),
            (DataValue::Timestamp(a), DataValue::Text(b)) => Some(a.cmp(b)),
            
            // Add other cross-type comparisons if necessary, e.g., Date vs Timestamp
            (DataValue::Date(a), DataValue::Timestamp(b)) => Some(a.as_str().cmp(b.split(' ').next().unwrap_or(""))),
            (DataValue::Timestamp(a), DataValue::Date(b)) => Some(a.split(' ').next().unwrap_or("").cmp(b.as_str())),

            _ => None,
        }
    }
}

impl DataValue {
    pub fn get_type(&self) -> crate::query::parser::ast::DataType {
        match self {
            DataValue::Null => crate::query::parser::ast::DataType::Text, 
            DataValue::Integer(_) => crate::query::parser::ast::DataType::Integer,
            DataValue::Float(_) => crate::query::parser::ast::DataType::Float,
            DataValue::Text(_) => crate::query::parser::ast::DataType::Text,
            DataValue::Boolean(_) => crate::query::parser::ast::DataType::Boolean,
            DataValue::Date(_) => crate::query::parser::ast::DataType::Date,
            DataValue::Timestamp(_) => crate::query::parser::ast::DataType::Timestamp,
            DataValue::Blob(_) => crate::query::parser::ast::DataType::Text, // Placeholder for AST Blob type
        }
    }

    pub fn is_compatible_with(&self, target_ast_type: &crate::query::parser::ast::DataType) -> bool {
        match self {
            DataValue::Null => true, 
            DataValue::Integer(_) => matches!(target_ast_type, crate::query::parser::ast::DataType::Integer | crate::query::parser::ast::DataType::Float | crate::query::parser::ast::DataType::Text),
            DataValue::Float(_) => matches!(target_ast_type, crate::query::parser::ast::DataType::Float | crate::query::parser::ast::DataType::Integer | crate::query::parser::ast::DataType::Text),
            DataValue::Text(s) => {
                match target_ast_type {
                    crate::query::parser::ast::DataType::Text => true,
                    crate::query::parser::ast::DataType::Integer => s.parse::<i64>().is_ok(),
                    crate::query::parser::ast::DataType::Float => s.parse::<f64>().is_ok(),
                    crate::query::parser::ast::DataType::Boolean => s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("false") || s.parse::<i64>().is_ok_and(|v| v == 0 || v == 1),
                    crate::query::parser::ast::DataType::Date => true, 
                    crate::query::parser::ast::DataType::Timestamp => true,
                    crate::query::parser::ast::DataType::Time => true, 
                }
            },
            DataValue::Boolean(_) => matches!(target_ast_type, crate::query::parser::ast::DataType::Boolean | crate::query::parser::ast::DataType::Text | crate::query::parser::ast::DataType::Integer),
            DataValue::Date(_) => matches!(target_ast_type, crate::query::parser::ast::DataType::Date | crate::query::parser::ast::DataType::Text | crate::query::parser::ast::DataType::Timestamp),
            DataValue::Timestamp(_) => matches!(target_ast_type, crate::query::parser::ast::DataType::Timestamp | crate::query::parser::ast::DataType::Text | crate::query::parser::ast::DataType::Date),
            DataValue::Blob(_) => matches!(target_ast_type, crate::query::parser::ast::DataType::Text), // Blob might be compatible with Text via hex representation
        }
    }

    pub fn serialize_row(values: &[DataValue]) -> QueryResult<Vec<u8>> {
        bincode::serialize(values).map_err(|e| QueryError::ExecutionError(format!("Failed to serialize row: {}", e)))
    }

    pub fn deserialize_row(bytes: &[u8], schema_for_len_check: &[crate::catalog::column::Column]) -> QueryResult<Vec<DataValue>> {
        let values: Vec<DataValue> = bincode::deserialize(bytes).map_err(|e| QueryError::ExecutionError(format!("Failed to deserialize row: {}", e)))?;
        // Basic check, could be more robust if schema contained full type info for deserialization context
        // RESTORED LENGTH CHECK
        if values.len() != schema_for_len_check.len() {
            // Depending on strictness, this could be an error or handled by padding with Null/truncating.
            // For now, returning an error to make it explicit.
            return Err(QueryError::ExecutionError(format!(
                "Deserialized row has {} values, but schema expects {}", 
                values.len(), schema_for_len_check.len()
            )));
        }
        Ok(values)
    }

    /// Compare two DataValues for sorting purposes.
    /// Handles NULLs (NULLs are considered less than any non-NULL value).
    /// Returns Ordering or QueryError for incompatible types.
    pub fn compare(&self, other: &Self) -> QueryResult<Ordering> {
        match (self, other) {
            (DataValue::Null, DataValue::Null) => Ok(Ordering::Equal),
            (DataValue::Null, _) => Ok(Ordering::Less), // Nulls first
            (_, DataValue::Null) => Ok(Ordering::Greater),
            // Non-null comparisons delegate to partial_cmp
            (a, b) => a.partial_cmp(b).ok_or_else(|| 
                QueryError::TypeError(format!("Cannot compare incompatible types: {:?} and {:?}", a.get_type(), b.get_type()))
            )
        }
    }

    pub fn to_sql_literal_for_error(&self) -> String {
        match self {
            DataValue::Null => "NULL".to_string(),
            DataValue::Integer(i) => i.to_string(),
            DataValue::Float(f) => f.to_string(),
            DataValue::Text(s) => format!("'{}'", s.replace("'", "''")), // Single quotes, escape internal single quotes
            DataValue::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.to_string(), // Match common SQL boolean literals
            DataValue::Date(s) => format!("'{}'", s), // Dates are often quoted
            DataValue::Timestamp(s) => format!("'{}'", s), // Timestamps often quoted
            DataValue::Blob(b) => format!("X'{}'", hex::encode(b)), // Standard SQL hex blob literal
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            DataValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }
}

/// Represents a row in query results
#[derive(Debug, Clone)]
pub struct Row {
    /// Values indexed by column name
    values: HashMap<String, DataValue>,
    /// Column order for consistent display
    column_order: Vec<String>,
}

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        // Two rows are equal if they have the same columns and values
        if self.column_order.len() != other.column_order.len() {
            return false;
        }

        // Check all values in this row exist in the other row
        for col in &self.column_order {
            match (self.values.get(col), other.values.get(col)) {
                (Some(v1), Some(v2)) if v1 == v2 => {}
                _ => return false,
            }
        }
        
        true
    }
}

impl Eq for Row {}

impl Default for Row {
    fn default() -> Self {
        Self::new()
    }
}

impl Row {
    /// Create a new empty row
    pub fn new() -> Self {
        Row {
            values: HashMap::new(),
            column_order: Vec::new(),
        }
    }
    
    /// Create a row from column values
    pub fn from_values(columns: Vec<String>, values: Vec<DataValue>) -> Self {
        let mut row = Row::new();
        row.column_order = columns.clone();
        
        for (col, val) in columns.into_iter().zip(values.into_iter()) {
            row.values.insert(col, val);
        }
        
        row
    }
    
    /// Create a row from an ordered map and explicit column order
    pub fn from_ordered_map(values: LinkedHashMap<String, DataValue>, column_order: Vec<String>) -> Self {
        // Convert LinkedHashMap to HashMap for storage
        let values_hashmap: HashMap<String, DataValue> = values.into_iter().collect();
        Row {
            values: values_hashmap,
            column_order,
        }
    }
    
    /// Get a value by column name
    pub fn get(&self, column: &str) -> Option<&DataValue> {
        self.values.get(column)
    }
    
    /// Set a value for a column
    pub fn set(&mut self, column: String, value: DataValue) {
        if !self.column_order.contains(&column) {
            self.column_order.push(column.clone());
        }
        self.values.insert(column, value);
    }
    
    /// Get all columns in the row
    pub fn columns(&self) -> &[String] {
        &self.column_order
    }
    
    /// Get all values in column order
    pub fn values(&self) -> Vec<&DataValue> {
        self.column_order.iter()
            .filter_map(|col| self.values.get(col))
            .collect()
    }
    
    /// Get all values with their corresponding column names
    pub fn values_with_names(&self) -> impl Iterator<Item = (&String, &DataValue)> {
        self.values.iter()
    }
}

/// Represents query execution error
#[derive(Error, Debug)]
pub enum QueryError {
    /// Error from storage layer
    #[error("Storage error: {0}")]
    StorageError(String),
    /// Error during query execution
    #[error("Execution error: {0}")]
    ExecutionError(String),
    /// Error in data type conversion
    #[error("Type error: {0}")]
    TypeError(String),
    /// Error during query planning phase
    #[error("Planning error: {0}")]
    PlanningError(String),
    /// Table not found
    #[error("Table not found: {0}")]
    TableNotFound(String),
    /// Column not found
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
    /// Schema not found
    #[error("Schema not found: {0}")]
    SchemaNotFound(String),
    /// Table already exists
    #[error("Table already exists: {0}")]
    TableAlreadyExists(String),
    /// Duplicate column
    #[error("Duplicate column: {0}")]
    DuplicateColumn(String),
    /// General catalog error
    #[error("Catalog error: {0}")]
    CatalogError(String),
    /// Transaction-related error
    #[error("Transaction error: {0}")]
    TransactionError(String),
    /// Invalid operation
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
    /// Numeric overflow
    #[error("Numeric overflow")]
    NumericOverflow,
    /// Division by zero
    #[error("Division by zero")]
    DivisionByZero,
}

// Implement From<ParseError> for QueryError
impl From<ParseError> for QueryError {
    fn from(err: ParseError) -> Self {
        QueryError::PlanningError(format!("Parse error: {:?}", err))
    }
}

// Implement From<PageError> for QueryError
impl From<PageError> for QueryError {
    fn from(err: PageError) -> Self {
        QueryError::StorageError(format!("Page error: {:?}", err))
    }
}

// Add this From implementation
impl From<BufferPoolError> for QueryError {
    fn from(err: BufferPoolError) -> Self {
        QueryError::StorageError(format!("Buffer pool error: {}", err))
    }
}

/// Result type for query operations
pub type QueryResult<T> = Result<T, QueryError>;

/// Query resultset representation
#[derive(Debug)]
pub struct QueryResultSet {
    /// Column names in the resultset
    columns: Vec<String>,
    /// Rows of data
    rows: Vec<Row>,
}

impl QueryResultSet {
    /// Create a new empty resultset with column names
    pub fn new(columns: Vec<String>) -> Self {
        QueryResultSet {
            columns,
            rows: Vec::new(),
        }
    }
    
    /// Add a row to the resultset
    pub fn add_row(&mut self, row: Row) {
        self.rows.push(row);
    }
    
    /// Get the columns in the resultset
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
    
    /// Get the rows in the resultset
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }
    
    /// Get the number of rows
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
    
    /// Format the resultset as a string table
    pub fn to_string_table(&self) -> String {
        if self.columns.is_empty() {
            return "Empty result".to_string();
        }
        
        let mut result = String::new();
        
        // Add column headers
        result.push_str("| ");
        for col in &self.columns {
            result.push_str(&format!("{} | ", col));
        }
        result.push('\n');
        
        // Add separator
        result.push('|');
        for col in &self.columns {
            result.push_str(&format!("{}|", "-".repeat(col.len() + 2)));
        }
        result.push('\n');
        
        // Add rows
        for row in &self.rows {
            result.push_str("| ");
            for col in &self.columns {
                if let Some(value) = row.get(col) {
                    result.push_str(&format!("{} | ", value));
                } else {
                    result.push_str("NULL | ");
                }
            }
            result.push('\n');
        }
        
        result
    }
    
    /// Return a message for empty result sets
    pub fn empty_message(&self) -> String {
        if self.columns.is_empty() {
            "Empty result".to_string()
        } else {
            format!("Empty result set with columns: {}", self.columns.join(", "))
        }
    }

    pub fn empty_ddl() -> Self {
        QueryResultSet {
            columns: vec!["status".to_string()],
            rows: vec![Row::from_values(vec!["status".to_string()], vec![DataValue::Text("DDL operation successful.".to_string())])],
        }
    }
}

/// Attempts to convert a DataValue to a target catalog DataType.
pub fn convert_data_value(
    value: &DataValue, 
    target_catalog_type: &crate::catalog::schema::DataType
) -> QueryResult<DataValue> {
    match target_catalog_type {
        crate::catalog::schema::DataType::Integer => match value {
            DataValue::Null => Ok(DataValue::Null),
            DataValue::Integer(i) => Ok(DataValue::Integer(*i)),
            DataValue::Float(f) => Ok(DataValue::Integer(*f as i64)), // Truncation
            DataValue::Text(s) => s.parse::<i64>()
                .map(DataValue::Integer)
                .map_err(|e| QueryError::TypeError(format!("Cannot convert Text '{}' to Integer: {}", s, e))),
            DataValue::Boolean(b) => Ok(DataValue::Integer(if *b { 1 } else { 0 })),
            DataValue::Date(s) => Err(QueryError::TypeError(format!("Cannot convert Date '{}' to Integer", s))),
            DataValue::Timestamp(s) => Err(QueryError::TypeError(format!("Cannot convert Timestamp '{}' to Integer", s))),
            DataValue::Blob(_) => Err(QueryError::TypeError("Cannot convert Blob to Integer".to_string())),
        },
        crate::catalog::schema::DataType::Float => match value {
            DataValue::Null => Ok(DataValue::Null),
            DataValue::Integer(i) => Ok(DataValue::Float(*i as f64)),
            DataValue::Float(f) => Ok(DataValue::Float(*f)),
            DataValue::Text(s) => s.parse::<f64>()
                .map(DataValue::Float)
                .map_err(|e| QueryError::TypeError(format!("Cannot convert Text '{}' to Float: {}", s, e))),
            DataValue::Boolean(b) => Ok(DataValue::Float(if *b { 1.0 } else { 0.0 })),
            DataValue::Date(s) => Err(QueryError::TypeError(format!("Cannot convert Date '{}' to Float", s))),
            DataValue::Timestamp(s) => Err(QueryError::TypeError(format!("Cannot convert Timestamp '{}' to Float", s))),
            DataValue::Blob(_) => Err(QueryError::TypeError("Cannot convert Blob to Float".to_string())),
        },
        crate::catalog::schema::DataType::Text => match value {
            DataValue::Null => Ok(DataValue::Null),
            DataValue::Integer(i) => Ok(DataValue::Text(i.to_string())),
            DataValue::Float(f) => Ok(DataValue::Text(f.to_string())),
            DataValue::Text(s) => Ok(DataValue::Text(s.clone())),
            DataValue::Boolean(b) => Ok(DataValue::Text(b.to_string())),
            DataValue::Date(s) => Ok(DataValue::Text(s.clone())),
            DataValue::Timestamp(s) => Ok(DataValue::Text(s.clone())),
            DataValue::Blob(b) => Ok(DataValue::Text(hex::encode(b))), // Example: Convert Blob to hex String
        },
        crate::catalog::schema::DataType::Boolean => match value {
            DataValue::Null => Ok(DataValue::Null),
            DataValue::Integer(i) => Ok(DataValue::Boolean(*i != 0)),
            DataValue::Float(f) => Ok(DataValue::Boolean(*f != 0.0)),
            DataValue::Text(s) => {
                if s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("t") || s.eq_ignore_ascii_case("1") {
                    Ok(DataValue::Boolean(true))
                } else if s.eq_ignore_ascii_case("false") || s.eq_ignore_ascii_case("f") || s.eq_ignore_ascii_case("0") {
                    Ok(DataValue::Boolean(false))
                } else {
                    Err(QueryError::TypeError(format!("Cannot convert Text '{}' to Boolean", s)))
                }
            },
            DataValue::Boolean(b) => Ok(DataValue::Boolean(*b)),
            DataValue::Date(s) => Err(QueryError::TypeError(format!("Cannot convert Date '{}' to Boolean", s))),
            DataValue::Timestamp(s) => Err(QueryError::TypeError(format!("Cannot convert Timestamp '{}' to Boolean", s))),
            DataValue::Blob(_) => Err(QueryError::TypeError("Cannot convert Blob to Boolean".to_string())),
        },
        crate::catalog::schema::DataType::Date => match value {
            DataValue::Null => Ok(DataValue::Null),
            DataValue::Text(s) => Ok(DataValue::Date(s.clone())), // Assume text is valid date string
            DataValue::Date(s) => Ok(DataValue::Date(s.clone())),
            DataValue::Timestamp(ts) => Ok(DataValue::Date(ts.split(' ').next().unwrap_or_default().to_string())),
            _ => Err(QueryError::TypeError(format!("Cannot convert {:?} to Date", value))),
        },
        crate::catalog::schema::DataType::Timestamp => match value {
            DataValue::Null => Ok(DataValue::Null),
            DataValue::Text(s) => Ok(DataValue::Timestamp(s.clone())), // Assume text is valid timestamp string
            DataValue::Date(s) => Ok(DataValue::Timestamp(format!("{} 00:00:00", s))),
            DataValue::Timestamp(ts) => Ok(DataValue::Timestamp(ts.clone())),
            _ => Err(QueryError::TypeError(format!("Cannot convert {:?} to Timestamp", value))),
        },
        crate::catalog::schema::DataType::Blob => match value {
            DataValue::Null => Ok(DataValue::Null),
            DataValue::Blob(b) => Ok(DataValue::Blob(b.clone())),
            DataValue::Text(s) => hex::decode(s) // Attempt to decode hex string to Vec<u8>
                .map(DataValue::Blob)
                .map_err(|e| QueryError::TypeError(format!("Cannot convert Text '{}' to Blob (hex decode failed): {}", s, e))),
            _ => Err(QueryError::TypeError(format!("Cannot convert {:?} to Blob", value))),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_row_operations() {
        let mut row = Row::new();
        row.set("id".to_string(), DataValue::Integer(1));
        row.set("name".to_string(), DataValue::Text("Test".to_string()));
        
        assert_eq!(row.get("id"), Some(&DataValue::Integer(1)));
        assert_eq!(row.get("name"), Some(&DataValue::Text("Test".to_string())));
        assert_eq!(row.get("missing"), None);
        
        // Columns should be in insertion order
        assert_eq!(row.columns(), &["id", "name"]);
    }
    
    #[test]
    fn test_result_set() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let mut result_set = QueryResultSet::new(columns);
        
        let row1 = Row::from_values(
            vec!["id".to_string(), "name".to_string()],
            vec![DataValue::Integer(1), DataValue::Text("John".to_string())]
        );
        
        let row2 = Row::from_values(
            vec!["id".to_string(), "name".to_string()],
            vec![DataValue::Integer(2), DataValue::Text("Jane".to_string())]
        );
        
        result_set.add_row(row1);
        result_set.add_row(row2);
        
        assert_eq!(result_set.row_count(), 2);
        assert_eq!(result_set.columns(), &["id", "name"]);
        
        // Check first row values
        if let Some(row) = result_set.rows().first() {
            assert_eq!(row.get("id"), Some(&DataValue::Integer(1)));
            assert_eq!(row.get("name"), Some(&DataValue::Text("John".to_string())));
        } else {
            panic!("Expected row at index 0");
        }
    }
} 