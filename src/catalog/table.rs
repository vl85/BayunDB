// Table Management Module
//
// This module defines the Table type that represents a database table schema.

use std::collections::HashMap;
use super::column::Column;
use serde::{Serialize, Deserialize};

/// Represents a database table schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    /// Table name
    name: String,
    /// Columns in the table
    columns: Vec<Column>,
    /// Column name to index lookup
    column_map: HashMap<String, usize>,
    /// Primary key column indices
    primary_key_columns: Vec<usize>,
    /// First page ID for this table's data
    first_page_id: Option<u32>,
}

impl Table {
    /// Create a new table with the given name and columns
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        let mut column_map = HashMap::new();
        let mut primary_key_columns = Vec::new();
        
        // Build column map and identify primary key columns
        for (i, col) in columns.iter().enumerate() {
            column_map.insert(col.name().to_string(), i);
            if col.is_primary_key() {
                primary_key_columns.push(i);
            }
        }
        
        Table {
            name,
            columns,
            column_map,
            primary_key_columns,
            first_page_id: None,
        }
    }
    
    /// Get the table name
    pub fn name(&self) -> &str {
        &self.name
    }
    
    /// Get all columns
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }
    
    /// Get a column by name
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.column_map.get(name).map(|&idx| &self.columns[idx])
    }
    
    /// Check if the table has a column with the given name
    pub fn has_column(&self, name: &str) -> bool {
        self.column_map.contains_key(name)
    }
    
    /// Get the column index for a column name
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.column_map.get(name).copied()
    }
    
    /// Get primary key columns
    pub fn primary_key_columns(&self) -> Vec<&Column> {
        self.primary_key_columns.iter()
            .map(|&idx| &self.columns[idx])
            .collect()
    }
    
    /// Set the first page ID for this table's data
    pub fn set_first_page_id(&mut self, page_id: u32) {
        self.first_page_id = Some(page_id);
    }
    
    /// Get the first page ID for this table's data
    pub fn first_page_id(&self) -> Option<u32> {
        self.first_page_id
    }
    
    /// Add a column to the table
    pub fn add_column(&mut self, column: Column) -> Result<(), String> {
        let col_name = column.name().to_string();
        
        if self.has_column(&col_name) {
            return Err(format!("Column {} already exists in table {}", col_name, self.name));
        }
        
        let idx = self.columns.len();
        self.columns.push(column);
        self.column_map.insert(col_name, idx);
        
        if self.columns[idx].is_primary_key() {
            self.primary_key_columns.push(idx);
        }
        
        Ok(())
    }
    
    /// Get the row size in bytes
    pub fn row_size(&self) -> usize {
        self.columns.iter().map(|col| col.size_in_bytes()).sum()
    }
    
    /// Get a string representation of the table schema
    pub fn schema_string(&self) -> String {
        let mut schema = format!("CREATE TABLE {} (\n", self.name);
        
        for (i, col) in self.columns.iter().enumerate() {
            schema.push_str(&format!("  {} {}", col.name(), col.data_type().to_string()));
            
            if !col.is_nullable() {
                schema.push_str(" NOT NULL");
            }
            
            if col.is_primary_key() {
                schema.push_str(" PRIMARY KEY");
            }
            
            if i < self.columns.len() - 1 {
                schema.push_str(",\n");
            }
        }
        
        schema.push_str("\n);");
        schema
    }
} 