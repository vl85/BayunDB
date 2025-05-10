//! Catalog Management Module
//!
//! This module manages the database schema metadata, including tables, columns, indexes, and other database objects.

pub mod schema;
pub mod table;
pub mod column;
pub mod validation;

// Re-export key types
pub use self::schema::Schema;
pub use self::table::Table;
pub use self::column::Column;
pub use self::schema::DataType;
pub use self::validation::{TypeValidator, ValidationError, ValidationResult};

use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU32, Ordering};
use crate::query::executor::result::QueryError;

// Global catalog instance using a thread-safe lazy initialization
static CATALOG_INSTANCE: Lazy<Arc<RwLock<Catalog>>> = Lazy::new(|| {
    let mut schemas = HashMap::new();
    
    // Create default schema
    let default_schema = Schema::new("public".to_string());
    schemas.insert("public".to_string(), default_schema);
    
    let catalog = Catalog {
        schemas: RwLock::new(schemas),
        current_schema: RwLock::new("public".to_string()),
        table_id_counter: AtomicU32::new(1),
    };
    
    Arc::new(RwLock::new(catalog))
});

/// The Catalog is the central repository for all database schema information
pub struct Catalog {
    /// Schemas in the database
    schemas: RwLock<HashMap<String, Schema>>,
    /// Current schema name
    current_schema: RwLock<String>,
    /// Counter for assigning unique table IDs
    table_id_counter: AtomicU32,
}

impl Catalog {
    /// Get the global catalog instance
    pub fn instance() -> Arc<RwLock<Catalog>> {
        CATALOG_INSTANCE.clone()
    }
    
    /// Create a new, empty catalog (primarily for testing)
    pub fn new() -> Self {
        let mut schemas = HashMap::new();
        
        // Create default schema
        let default_schema = Schema::new("public".to_string());
        schemas.insert("public".to_string(), default_schema);
        
        Catalog {
            schemas: RwLock::new(schemas),
            current_schema: RwLock::new("public".to_string()),
            table_id_counter: AtomicU32::new(1),
        }
    }
    
    /// Get a reference to a schema by name
    #[allow(dead_code)]
    pub(crate) fn get_schema(&self, name: &str) -> Option<Schema> {
        self.schemas.read().unwrap().get(name).cloned()
    }
    
    /// Get the current schema
    #[allow(dead_code)]
    pub(crate) fn current_schema(&self) -> Schema {
        let current_name = self.current_schema.read().unwrap().clone();
        self.get_schema(&current_name).unwrap()
    }
    
    /// Create a new schema
    #[allow(dead_code)]
    pub(crate) fn create_schema(&self, name: String) -> Result<(), String> {
        let mut schemas = self.schemas.write().unwrap();
        if schemas.contains_key(&name) {
            return Err(format!("Schema {} already exists", name));
        }
        
        let schema = Schema::new(name.clone());
        schemas.insert(name, schema);
        Ok(())
    }
    
    /// Set the current schema
    #[allow(dead_code)]
    pub(crate) fn set_current_schema(&self, name: String) -> Result<(), String> {
        let schemas = self.schemas.read().unwrap();
        if !schemas.contains_key(&name) {
            return Err(format!("Schema {} does not exist", name));
        }
        
        let mut current = self.current_schema.write().unwrap();
        *current = name;
        Ok(())
    }
    
    /// Create a table in the current schema
    pub fn create_table(&self, mut table: Table) -> Result<(), String> {
        let schema_name = self.current_schema.read().unwrap().clone();
        let mut schemas_guard = self.schemas.write().unwrap();
        
        if let Some(schema) = schemas_guard.get_mut(&schema_name) {
            let new_id = self.table_id_counter.fetch_add(1, Ordering::SeqCst);
            table.set_id(new_id);
            schema.add_table(table)
        } else {
            Err(format!("Schema {} does not exist", schema_name))
        }
    }
    
    /// Check if a table exists in the current schema
    pub fn table_exists(&self, table_name: &str) -> bool {
        let schema_name = self.current_schema.read().unwrap().clone();
        let schemas = self.schemas.read().unwrap();
        
        if let Some(schema) = schemas.get(&schema_name) {
            schema.has_table(table_name)
        } else {
            false
        }
    }
    
    /// Get a table from the current schema
    pub fn get_table(&self, table_name: &str) -> Option<Table> {
        let schema_name = self.current_schema.read().unwrap().clone();
        let schemas = self.schemas.read().unwrap();
        
        if let Some(schema) = schemas.get(&schema_name) {
            schema.get_table(table_name)
        } else {
            None
        }
    }

    /// Get a mutable reference to a table in the current schema.
    /// This requires the caller to have a mutable reference to the Catalog.
    pub fn get_table_mut_from_current_schema(&mut self, table_name: &str) -> Option<&mut Table> {
        let current_schema_name = self.current_schema.read().unwrap().clone();
        self.schemas.get_mut().unwrap()
            .get_mut(&current_schema_name) 
            .and_then(|schema| schema.get_table_mut(table_name))
    }

    /// Drop a table from the current schema.
    /// This requires the caller to have a mutable reference to the Catalog.
    pub fn drop_table_from_current_schema(&mut self, table_name: &str) -> Result<(), String> {
        let current_schema_name = self.current_schema.read().unwrap().clone();
        let mut schemas_guard = self.schemas.write().unwrap();
        match schemas_guard.get_mut(&current_schema_name) {
            Some(schema) => schema.drop_table(table_name),
            None => Err(format!("Schema '{}' not found when trying to drop table {}", current_schema_name, table_name)),
        }
    }

    /// Retrieves a mutable reference to a table by name from a specific schema.
    // Added this method as a public alternative if needed, though the current errors are for the one above.
    // pub fn get_table_mut_from_schema(&mut self, schema_name: &str, table_name: &str) -> Option<&mut Table> {
    //     self.schemas.get_mut(schema_name)
    //         .and_then(|schema| schema.get_table_mut(table_name))
    // }

    /// Get the ID of a table in the current schema
    pub fn get_table_id(&self, table_name: &str) -> Option<u32> {
        let schema_name = self.current_schema.read().unwrap().clone();
        let schemas = self.schemas.read().unwrap();
        
        if let Some(schema) = schemas.get(&schema_name) {
            schema.get_table(table_name).map(|t| t.id())
        } else {
            None
        }
    }

    /// ALTER TABLE: Add a column to a table in the current schema
    pub fn alter_table_add_column(&self, table_name: &str, column: Column) -> Result<(), QueryError> {
        let schema_name = self.current_schema.read().unwrap().clone();
        let mut schemas_guard = self.schemas.write().unwrap();
        if let Some(schema) = schemas_guard.get_mut(&schema_name) {
            if let Some(table) = schema.get_table_mut(table_name) {
                // The Table::add_column method itself returns Result<(), String>
                // We need to map this error if it occurs.
                table.add_column(column).map_err(QueryError::CatalogError)
            } else {
                Err(QueryError::TableNotFound(table_name.to_string()))
            }
        } else {
            // This case should ideally not be reachable if current_schema is always valid.
            Err(QueryError::SchemaNotFound(schema_name))
        }
    }
    
    /// ALTER TABLE: Drop a column from a table in the current schema
    pub fn alter_table_drop_column(&self, table_name: &str, column_name: &str) -> Result<(), QueryError> {
        let schema_name = self.current_schema.read().unwrap().clone();
        let mut schemas_guard = self.schemas.write().unwrap();
        if let Some(schema) = schemas_guard.get_mut(&schema_name) {
            if let Some(table) = schema.get_table_mut(table_name) {
                // Check if this is the last column and if it's the one being dropped
                if table.columns().len() == 1 {
                    // Verify that the column_name to be dropped is indeed this last column.
                    // table.columns().first() is safe because we checked len() == 1.
                    if table.columns().first().unwrap().name() == column_name {
                        return Err(QueryError::ExecutionError(format!(
                            "Cannot drop the last column '{}' from table '{}'. Use DROP TABLE instead.",
                            column_name, table_name
                        )));
                    }
                    // If it's the last column but the name doesn't match,
                    // table.drop_column() below will correctly return a ColumnNotFound error.
                }

                // Original logic: The Table::drop_column method returns Result<(), String>
                // We need to map this error (e.g. ColumnNotFound from table.rs)
                table.drop_column(column_name).map_err(|e_str| {
                    if e_str.contains("not found") { // Heuristic, ideally Table::drop_column returns a typed error
                        QueryError::ColumnNotFound(format!("Column '{}' not found in table '{}'", column_name, table_name))
                    } else {
                        // Pass through other errors from table.drop_column (e.g. if it had other internal errors)
                        QueryError::CatalogError(e_str)
                    }
                })
            } else {
                Err(QueryError::TableNotFound(table_name.to_string()))
            }
        } else {
            Err(QueryError::SchemaNotFound(schema_name))
        }
    }

    /// ALTER TABLE: Rename a column in a table in the current schema
    pub fn alter_table_rename_column(&self, table_name: &str, old_name: &str, new_name: &str) -> Result<(), QueryError> {
        let schema_name = self.current_schema.read().unwrap().clone();
        let mut schemas_guard = self.schemas.write().unwrap();
        if let Some(schema) = schemas_guard.get_mut(&schema_name) {
            if let Some(table) = schema.get_table_mut(table_name) {
                // The Table::rename_column method returns Result<(), String>
                table.rename_column(old_name, new_name).map_err(|e_str| {
                     if e_str.contains("not found") && e_str.contains(old_name) { // Heuristic
                        QueryError::ColumnNotFound(format!("Column '{}' not found in table '{}'", old_name, table_name))
                    } else if e_str.contains("already exists") && e_str.contains(new_name) {
                        QueryError::DuplicateColumn(new_name.to_string())
                    }
                    else {
                        QueryError::CatalogError(e_str)
                    }
                })
            } else {
                Err(QueryError::TableNotFound(table_name.to_string()))
            }
        } else {
            Err(QueryError::SchemaNotFound(schema_name))
        }
    }

    /// ALTER TABLE: Alter the data type of a column in a table in the current schema
    pub fn alter_table_alter_column_type(
        &self, 
        table_name: &str, 
        column_name: &str, 
        new_ast_data_type: &crate::query::parser::ast::DataType
    ) -> Result<(), QueryError> {
        let schema_name = self.current_schema.read().unwrap().clone();
        let mut schemas_guard = self.schemas.write().unwrap();
        
        if let Some(schema) = schemas_guard.get_mut(&schema_name) {
            if let Some(table) = schema.get_table_mut(table_name) {
                // Convert AST DataType to catalog::DataType
                let new_catalog_data_type = match new_ast_data_type {
                    crate::query::parser::ast::DataType::Integer => schema::DataType::Integer,
                    crate::query::parser::ast::DataType::Float => schema::DataType::Float,
                    crate::query::parser::ast::DataType::Text => schema::DataType::Text,
                    crate::query::parser::ast::DataType::Boolean => schema::DataType::Boolean,
                    crate::query::parser::ast::DataType::Date => schema::DataType::Date,
                    crate::query::parser::ast::DataType::Timestamp => schema::DataType::Timestamp,
                    // Add other types as they are supported
                };
                // The Table::alter_column_type method returns Result<(), String>
                table.alter_column_type(column_name, new_catalog_data_type).map_err(|e_str| {
                    if e_str.contains("not found") { // Heuristic
                        QueryError::ColumnNotFound(format!("Column '{}' not found in table '{}'", column_name, table_name))
                    } else {
                        QueryError::CatalogError(e_str)
                    }
                })
            } else {
                Err(QueryError::TableNotFound(table_name.to_string()))
            }
        } else {
            Err(QueryError::SchemaNotFound(schema_name))
        }
    }
    
    /// Helper to get a clone of a table's schema (columns) for use in the executor
    /// This is useful when the executor needs schema info but shouldn't hold a lock on the catalog.
    pub fn get_table_schema(&self, table_name: &str) -> Result<Table, QueryError> {
        let schema_name = self.current_schema.read().unwrap().clone();
        let schemas_guard = self.schemas.read().unwrap(); // Read lock is fine
        
        if let Some(schema) = schemas_guard.get(&schema_name) {
            if let Some(table) = schema.get_table(table_name) {
                Ok(table.clone()) // Clone the table definition
            } else {
                Err(QueryError::TableNotFound(table_name.to_string()))
            }
        } else {
            Err(QueryError::SchemaNotFound(schema_name))
        }
    }
}

// Tests module
#[cfg(test)]
mod tests {
// // ... existing code ...
} 