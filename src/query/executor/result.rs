// Query Result Implementation
//
// This module defines the result types for query execution.

use std::collections::HashMap;
use std::fmt;

/// Possible data types for values in a row
#[derive(Debug, Clone, PartialEq)]
pub enum DataValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Boolean(bool),
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

/// Represents a row in query results
#[derive(Debug, Clone)]
pub struct Row {
    /// Values indexed by column name
    values: HashMap<String, DataValue>,
    /// Column order for consistent display
    column_order: Vec<String>,
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
}

/// Represents query execution error
#[derive(Debug)]
pub enum QueryError {
    /// Error from storage layer
    StorageError(String),
    /// Error during query execution
    ExecutionError(String),
    /// Error in data type conversion
    TypeError(String),
    /// Table not found
    TableNotFound(String),
    /// Column not found
    ColumnNotFound(String),
    /// Invalid operation
    InvalidOperation(String),
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            QueryError::ExecutionError(msg) => write!(f, "Execution error: {}", msg),
            QueryError::TypeError(msg) => write!(f, "Type error: {}", msg),
            QueryError::TableNotFound(table) => write!(f, "Table not found: {}", table),
            QueryError::ColumnNotFound(col) => write!(f, "Column not found: {}", col),
            QueryError::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
        }
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