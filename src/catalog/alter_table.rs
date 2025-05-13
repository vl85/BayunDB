use super::catalog::Catalog;
use super::column::Column;
use crate::query::executor::result::QueryError;

impl Catalog {
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
                    crate::query::parser::ast::DataType::Integer => super::schema::DataType::Integer,
                    crate::query::parser::ast::DataType::Float => super::schema::DataType::Float,
                    crate::query::parser::ast::DataType::Text => super::schema::DataType::Text,
                    crate::query::parser::ast::DataType::Boolean => super::schema::DataType::Boolean,
                    crate::query::parser::ast::DataType::Date => super::schema::DataType::Date,
                    crate::query::parser::ast::DataType::Timestamp => super::schema::DataType::Timestamp,
                    crate::query::parser::ast::DataType::Time => super::schema::DataType::Text,
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
} 