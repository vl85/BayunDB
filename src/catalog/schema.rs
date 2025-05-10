// Schema Management Module
//
// This module defines the Schema type that represents a database schema.

use std::collections::HashMap;
use super::table::Table;
use serde::{Serialize, Deserialize};

/// Data types supported by the database
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Integer,
    Float,
    Text,
    Boolean,
    Date,
    Timestamp,
    Blob,
    // Add more data types as needed
}

impl DataType {
    /// Convert a string representation to a DataType
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s.to_uppercase().as_str() {
            "INT" | "INTEGER" => Ok(DataType::Integer),
            "FLOAT" | "REAL" | "DOUBLE" => Ok(DataType::Float),
            "TEXT" | "VARCHAR" | "CHAR" | "STRING" => Ok(DataType::Text),
            "BOOL" | "BOOLEAN" => Ok(DataType::Boolean),
            "DATE" => Ok(DataType::Date),
            "TIMESTAMP" | "DATETIME" => Ok(DataType::Timestamp),
            "BLOB" | "BINARY" => Ok(DataType::Blob),
            _ => Err(format!("Unknown data type: {}", s)),
        }
    }
    
    /// Get the size in bytes of this data type
    pub fn size_in_bytes(&self) -> usize {
        match self {
            DataType::Integer => 8,     // 64-bit integer
            DataType::Float => 8,       // 64-bit float
            DataType::Boolean => 1,     // Single byte
            DataType::Date => 4,        // 32-bit integer for days since epoch
            DataType::Timestamp => 8,   // 64-bit integer for milliseconds
            DataType::Text => 0,        // Variable size
            DataType::Blob => 0,        // Variable size
        }
    }
    
    /// Check if this is a fixed-size data type
    pub fn is_fixed_size(&self) -> bool {
        match self {
            DataType::Text | DataType::Blob => false,
            _ => true,
        }
    }
    
    /// Convert to string representation
    pub fn to_string(&self) -> String {
        match self {
            DataType::Integer => "INTEGER".to_string(),
            DataType::Float => "FLOAT".to_string(),
            DataType::Text => "TEXT".to_string(),
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::Date => "DATE".to_string(),
            DataType::Timestamp => "TIMESTAMP".to_string(),
            DataType::Blob => "BLOB".to_string(),
        }
    }
}

/// Represents a database schema, which is a collection of tables
#[derive(Debug, Clone)]
pub struct Schema {
    /// Schema name
    name: String,
    /// Tables in this schema
    tables: HashMap<String, Table>,
}

impl Schema {
    /// Create a new, empty schema
    pub(crate) fn new(name: String) -> Self {
        Schema {
            name,
            tables: HashMap::new(),
        }
    }
    
    /// Get the schema name
    pub fn name(&self) -> &str {
        &self.name
    }
    
    /// Add a table to the schema
    pub(crate) fn add_table(&mut self, table: Table) -> Result<(), String> {
        let table_name = table.name().to_string();
        
        if self.tables.contains_key(&table_name) {
            return Err(format!("Table {} already exists in schema {}", table_name, self.name));
        }
        
        self.tables.insert(table_name, table);
        Ok(())
    }
    
    /// Check if a table exists in this schema
    pub fn has_table(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name)
    }
    
    /// Get a table by name
    pub fn get_table(&self, table_name: &str) -> Option<Table> {
        self.tables.get(table_name).cloned()
    }

    pub(crate) fn get_table_mut(&mut self, table_name: &str) -> Option<&mut Table> {
        self.tables.get_mut(table_name)
    }
    
    /// Get all tables in this schema
    pub fn tables(&self) -> Vec<Table> {
        self.tables.values().cloned().collect()
    }
    
    /// Drop a table from the schema
    #[allow(dead_code)]
    pub(crate) fn drop_table(&mut self, table_name: &str) -> Result<(), String> {
        if self.tables.contains_key(table_name) {
            self.tables.remove(table_name);
        }
        Ok(())
    }
} 