// Column Management Module
//
// This module defines the Column type that represents a database column schema.

use super::schema::DataType;
use serde::{Serialize, Deserialize};
use crate::query::parser::ast::Value as AstValue; // Import AstValue

/// Represents a column in a database table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    /// Column name
    name: String,
    /// Column data type
    data_type: DataType,
    /// Whether this column can contain NULL values
    nullable: bool,
    /// Whether this column is part of the primary key
    primary_key: bool,
    /// Default value (if any)
    pub(crate) default_ast_literal: Option<AstValue>, // New: Stores AST Literal if default is a literal
}

impl Column {
    /// Create a new column
    pub fn new(
        name: String,
        data_type: DataType,
        nullable: bool,
        primary_key: bool,
        default_ast_literal: Option<AstValue>, // New
    ) -> Self {
        Column {
            name,
            data_type,
            nullable,
            primary_key,
            default_ast_literal, // New
        }
    }
    
    /// Get the column name
    pub fn name(&self) -> &str {
        &self.name
    }
    
    /// Get the column data type
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
    
    /// Check if the column can contain NULL values
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }
    
    /// Check if the column is part of the primary key
    pub fn is_primary_key(&self) -> bool {
        self.primary_key
    }
    
    /// Get the default value (if any)
    pub fn get_default_ast_literal(&self) -> Option<&AstValue> { // New
        self.default_ast_literal.as_ref()
    }
    
    /// Get the size of this column in bytes
    pub fn size_in_bytes(&self) -> usize {
        // For variable-length types, we'll use a placeholder
        // In a real implementation, this would depend on the maximum size
        match self.data_type {
            DataType::Text => 256,  // Placeholder for average text field
            DataType::Blob => 1024, // Placeholder for average BLOB
            _ => self.data_type.size_in_bytes(),
        }
    }
    
    /// Create a column from a column definition
    pub(crate) fn from_column_def(col_def: &crate::query::parser::ast::ColumnDef) -> Result<Self, String> {
        // Convert AST DataType to catalog DataType
        let data_type = match col_def.data_type {
            crate::query::parser::ast::DataType::Integer => DataType::Integer,
            crate::query::parser::ast::DataType::Float => DataType::Float,
            crate::query::parser::ast::DataType::Text => DataType::Text,
            crate::query::parser::ast::DataType::Boolean => DataType::Boolean,
            crate::query::parser::ast::DataType::Date => DataType::Date,
            crate::query::parser::ast::DataType::Timestamp => DataType::Timestamp,
        };
        
        let default_literal = if let Some(expr) = &col_def.default_value {
            if let crate::query::parser::ast::Expression::Literal(val) = expr {
                Some(val.clone()) 
            } else {
                // Non-literal default expressions are not stored as AstValue for now.
                // This path would be hit if default is, e.g., a function call.
                // For now, this means no default will be applied by prepare_row_for_insert for such columns.
                None 
            }
        } else {
            None
        };

        Ok(Column {
            name: col_def.name.clone(),
            data_type,
            nullable: col_def.nullable,
            primary_key: col_def.primary_key,
            default_ast_literal: default_literal,
        })
    }

    /// Rename the column
    pub(crate) fn rename(&mut self, new_name: &str) {
        self.name = new_name.to_string();
    }
} 