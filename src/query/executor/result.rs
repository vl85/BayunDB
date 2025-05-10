// Query Result Implementation
//
// This module defines the result types for query execution.

use std::collections::HashMap;
use std::fmt;
use std::cmp::{Ordering, Eq};
use std::hash::{Hash, Hasher};
use serde;
use thiserror::Error;

use crate::query::parser::components::ParseError;
use crate::storage::page::PageError;

/// Possible data types for values in a row
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DataValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Boolean(bool),
}

impl Eq for DataValue {}

impl Hash for DataValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Add a type discriminant first to avoid collisions between different types
        match self {
            DataValue::Null => {
                0.hash(state);
            }
            DataValue::Integer(i) => {
                1.hash(state);
                i.hash(state);
            }
            DataValue::Float(f) => {
                2.hash(state);
                // Handle NaN and -0.0 special cases
                let bits = f.to_bits();
                bits.hash(state);
            }
            DataValue::Text(s) => {
                3.hash(state);
                s.hash(state);
            }
            DataValue::Boolean(b) => {
                4.hash(state);
                b.hash(state);
            }
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
        }
    }
}

impl PartialOrd for DataValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            // Null is incomparable with anything
            (DataValue::Null, _) | (_, DataValue::Null) => None,
            
            // Compare integers
            (DataValue::Integer(a), DataValue::Integer(b)) => a.partial_cmp(b),
            
            // Compare floats
            (DataValue::Float(a), DataValue::Float(b)) => a.partial_cmp(b),
            
            // Integer and float can be compared
            (DataValue::Integer(a), DataValue::Float(b)) => (*a as f64).partial_cmp(b),
            (DataValue::Float(a), DataValue::Integer(b)) => a.partial_cmp(&(*b as f64)),
            
            // Compare strings
            (DataValue::Text(a), DataValue::Text(b)) => a.partial_cmp(b),
            
            // Compare booleans
            (DataValue::Boolean(a), DataValue::Boolean(b)) => a.partial_cmp(b),
            
            // Different types are incomparable (except int/float)
            _ => None,
        }
    }
}

impl DataValue {
    pub fn get_type(&self) -> crate::query::parser::ast::DataType {
        match self {
            DataValue::Null => crate::query::parser::ast::DataType::Text, // Or a special "NullType"
            DataValue::Integer(_) => crate::query::parser::ast::DataType::Integer,
            DataValue::Float(_) => crate::query::parser::ast::DataType::Float,
            DataValue::Text(_) => crate::query::parser::ast::DataType::Text,
            DataValue::Boolean(_) => crate::query::parser::ast::DataType::Boolean,
        }
    }

    pub fn is_compatible_with(&self, target_ast_type: &crate::query::parser::ast::DataType) -> bool {
        match self {
            DataValue::Null => true, // Null is compatible with any nullable column
            DataValue::Integer(_) => matches!(target_ast_type, crate::query::parser::ast::DataType::Integer | crate::query::parser::ast::DataType::Float),
            DataValue::Float(_) => matches!(target_ast_type, crate::query::parser::ast::DataType::Float | crate::query::parser::ast::DataType::Integer),
            DataValue::Text(s) => {
                match target_ast_type {
                    crate::query::parser::ast::DataType::Text => true,
                    crate::query::parser::ast::DataType::Integer => s.parse::<i64>().is_ok(),
                    crate::query::parser::ast::DataType::Float => s.parse::<f64>().is_ok(),
                    crate::query::parser::ast::DataType::Boolean => s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("false"),
                    // Add other types like Date, Timestamp if they become parseable from Text
                    _ => false,
                }
            },
            DataValue::Boolean(_) => matches!(target_ast_type, crate::query::parser::ast::DataType::Boolean | crate::query::parser::ast::DataType::Text),
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
    /// Invalid operation
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
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
        result.push_str("|");
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
        },
        crate::catalog::schema::DataType::Float => match value {
            DataValue::Null => Ok(DataValue::Null),
            DataValue::Integer(i) => Ok(DataValue::Float(*i as f64)),
            DataValue::Float(f) => Ok(DataValue::Float(*f)),
            DataValue::Text(s) => s.parse::<f64>()
                .map(DataValue::Float)
                .map_err(|e| QueryError::TypeError(format!("Cannot convert Text '{}' to Float: {}", s, e))),
            DataValue::Boolean(b) => Ok(DataValue::Float(if *b { 1.0 } else { 0.0 })),
        },
        crate::catalog::schema::DataType::Text => match value {
            DataValue::Null => Ok(DataValue::Null),
            DataValue::Integer(i) => Ok(DataValue::Text(i.to_string())),
            DataValue::Float(f) => Ok(DataValue::Text(f.to_string())),
            DataValue::Text(s) => Ok(DataValue::Text(s.clone())),
            DataValue::Boolean(b) => Ok(DataValue::Text(b.to_string())),
        },
        crate::catalog::schema::DataType::Boolean => match value {
            DataValue::Null => Ok(DataValue::Null),
            DataValue::Integer(i) => Ok(DataValue::Boolean(*i != 0)),
            DataValue::Float(f) => Ok(DataValue::Boolean(*f != 0.0)), // Consider precision for float to bool
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
        },
        crate::catalog::schema::DataType::Date | crate::catalog::schema::DataType::Timestamp | crate::catalog::schema::DataType::Blob => {
            // DataValue does not yet support these types directly.
            // If the source is Null, it can be Null in the target.
            if matches!(value, DataValue::Null) {
                Ok(DataValue::Null)
            } else {
                 Err(QueryError::TypeError(format!(
                    "Conversion from {:?} to catalog type {:?} is not supported by DataValue yet.", 
                    value, target_catalog_type
                )))
            }
        }
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
        if let Some(row) = result_set.rows().get(0) {
            assert_eq!(row.get("id"), Some(&DataValue::Integer(1)));
            assert_eq!(row.get("name"), Some(&DataValue::Text("John".to_string())));
        } else {
            panic!("Expected row at index 0");
        }
    }
} 