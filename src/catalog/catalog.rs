use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU32, Ordering};
use crate::query::executor::result::QueryError;
use super::schema::{Schema, DataType};
use super::table::Table;
use super::column::Column;

// Global catalog instance using a thread-safe lazy initialization
static CATALOG_INSTANCE: Lazy<Arc<RwLock<Catalog>>> = Lazy::new(|| {
    let mut schemas = HashMap::new();
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
    pub(crate) schemas: RwLock<HashMap<String, Schema>>,
    pub(crate) current_schema: RwLock<String>,
    pub(crate) table_id_counter: AtomicU32,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

impl Catalog {
    pub fn instance() -> Arc<RwLock<Catalog>> {
        CATALOG_INSTANCE.clone()
    }
    pub fn new() -> Self {
        let mut schemas = HashMap::new();
        let default_schema = Schema::new("public".to_string());
        schemas.insert("public".to_string(), default_schema);
        Catalog {
            schemas: RwLock::new(schemas),
            current_schema: RwLock::new("public".to_string()),
            table_id_counter: AtomicU32::new(1),
        }
    }
    pub(crate) fn get_schema(&self, name: &str) -> Option<Schema> {
        self.schemas.read().unwrap().get(name).cloned()
    }
    pub(crate) fn current_schema(&self) -> Schema {
        let current_name = self.current_schema.read().unwrap().clone();
        self.get_schema(&current_name).unwrap()
    }
    pub(crate) fn create_schema(&self, name: String) -> Result<(), String> {
        let mut schemas = self.schemas.write().unwrap();
        if schemas.contains_key(&name) {
            return Err(format!("Schema {} already exists", name));
        }
        let schema = Schema::new(name.clone());
        schemas.insert(name, schema);
        Ok(())
    }
    pub(crate) fn set_current_schema(&self, name: String) -> Result<(), String> {
        let schemas = self.schemas.read().unwrap();
        if !schemas.contains_key(&name) {
            return Err(format!("Schema {} does not exist", name));
        }
        let mut current = self.current_schema.write().unwrap();
        *current = name;
        Ok(())
    }
    pub fn create_table(&self, mut table: Table) -> Result<(), QueryError> {
        let schema_name = self.current_schema.read().unwrap().clone();
        let mut schemas_guard = self.schemas.write().unwrap();
        if let Some(schema) = schemas_guard.get_mut(&schema_name) {
            let new_id = self.table_id_counter.fetch_add(1, Ordering::SeqCst);
            table.set_id(new_id);
            schema.add_table(table.clone()).map_err(|e_str| {
                if e_str.to_lowercase().contains("already exists") {
                    QueryError::TableAlreadyExists(table.name().to_string())
                } else {
                    QueryError::CatalogError(e_str)
                }
            })
        } else {
            Err(QueryError::SchemaNotFound(schema_name))
        }
    }
    pub fn table_exists(&self, table_name: &str) -> bool {
        let schema_name = self.current_schema.read().unwrap().clone();
        let schemas = self.schemas.read().unwrap();
        if let Some(schema) = schemas.get(&schema_name) {
            schema.has_table(table_name)
        } else {
            false
        }
    }
    pub fn get_table(&self, table_name: &str) -> Option<Table> {
        let schema_name = self.current_schema.read().unwrap().clone();
        let schemas = self.schemas.read().unwrap();
        if let Some(schema) = schemas.get(&schema_name) {
            schema.get_table(table_name)
        } else {
            None
        }
    }
    pub fn get_table_mut_from_current_schema(&mut self, table_name: &str) -> Option<&mut Table> {
        let current_schema_name = self.current_schema.read().unwrap().clone();
        self.schemas.get_mut().unwrap()
            .get_mut(&current_schema_name) 
            .and_then(|schema| schema.get_table_mut(table_name))
    }
    pub fn drop_table_from_current_schema(&mut self, table_name: &str) -> Result<(), String> {
        let current_schema_name = self.current_schema.read().unwrap().clone();
        let mut schemas_guard = self.schemas.write().unwrap();
        match schemas_guard.get_mut(&current_schema_name) {
            Some(schema) => schema.drop_table(table_name),
            None => Err(format!("Schema '{}' not found when trying to drop table {}", current_schema_name, table_name)),
        }
    }
    pub fn get_table_id(&self, table_name: &str) -> Option<u32> {
        let schema_name = self.current_schema.read().unwrap().clone();
        let schemas = self.schemas.read().unwrap();
        if let Some(schema) = schemas.get(&schema_name) {
            schema.get_table(table_name).map(|t| t.id())
        } else {
            None
        }
    }
    pub fn alter_table_add_column(&self, table_name: &str, column: Column) -> Result<(), QueryError> {
        let schema_name = self.current_schema.read().unwrap().clone();
        let mut schemas_guard = self.schemas.write().unwrap();
        if let Some(schema) = schemas_guard.get_mut(&schema_name) {
            if let Some(table) = schema.get_table_mut(table_name) {
                table.add_column(column).map_err(QueryError::CatalogError)
            } else {
                Err(QueryError::TableNotFound(table_name.to_string()))
            }
        } else {
            Err(QueryError::SchemaNotFound(schema_name))
        }
    }
    pub fn alter_table_drop_column(&self, table_name: &str, column_name: &str) -> Result<(), QueryError> {
        let schema_name = self.current_schema.read().unwrap().clone();
        let mut schemas_guard = self.schemas.write().unwrap();
        if let Some(schema) = schemas_guard.get_mut(&schema_name) {
            if let Some(table) = schema.get_table_mut(table_name) {
                if table.columns().len() == 1 {
                    if table.columns().first().unwrap().name() == column_name {
                        return Err(QueryError::ExecutionError(format!(
                            "Cannot drop the last column '{}' from table '{}'. Use DROP TABLE instead.",
                            column_name, table_name
                        )));
                    }
                }
                table.drop_column(column_name).map_err(|e_str| {
                    if e_str.contains("not found") {
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
    pub fn alter_table_rename_column(&self, table_name: &str, old_name: &str, new_name: &str) -> Result<(), QueryError> {
        let schema_name = self.current_schema.read().unwrap().clone();
        let mut schemas_guard = self.schemas.write().unwrap();
        if let Some(schema) = schemas_guard.get_mut(&schema_name) {
            if let Some(table) = schema.get_table_mut(table_name) {
                table.rename_column(old_name, new_name).map_err(|e_str| {
                     if e_str.contains("not found") && e_str.contains(old_name) {
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
                let new_catalog_data_type = match new_ast_data_type {
                    crate::query::parser::ast::DataType::Integer => DataType::Integer,
                    crate::query::parser::ast::DataType::Float => DataType::Float,
                    crate::query::parser::ast::DataType::Text => DataType::Text,
                    crate::query::parser::ast::DataType::Boolean => DataType::Boolean,
                    crate::query::parser::ast::DataType::Date => DataType::Date,
                    crate::query::parser::ast::DataType::Timestamp => DataType::Timestamp,
                    crate::query::parser::ast::DataType::Time => DataType::Text,
                };
                table.alter_column_type(column_name, new_catalog_data_type).map_err(|e_str| {
                    if e_str.contains("not found") {
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
    pub fn get_table_schema(&self, table_name: &str) -> Result<Table, QueryError> {
        let schema_name = self.current_schema.read().unwrap().clone();
        let schemas_guard = self.schemas.read().unwrap();
        if let Some(schema) = schemas_guard.get(&schema_name) {
            if let Some(table) = schema.get_table(table_name) {
                Ok(table.clone())
            } else {
                Err(QueryError::TableNotFound(table_name.to_string()))
            }
        } else {
            Err(QueryError::SchemaNotFound(schema_name))
        }
    }
} 