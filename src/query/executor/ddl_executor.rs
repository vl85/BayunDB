// Data Definition Language Executor
//
// This module handles execution of DDL statements like CREATE, ALTER.

use std::sync::Arc;
use std::sync::RwLock;
use std::io::Write;
use bincode;
use std::sync::MutexGuard;

use crate::storage::buffer::BufferPoolManager;
use crate::storage::page::PageManager;
use crate::storage::page::PageError;
use crate::query::executor::result::{QueryResult, QueryError, QueryResultSet, DataValue, Row};
use crate::query::parser::ast::{CreateStatement, AlterTableStatement, ColumnDef};
use crate::catalog::Catalog;
use crate::catalog::{Table, Column};
use crate::common::types::{Rid};
use crate::query::executor::type_conversion::{helper_ast_dt_to_catalog_dt, ast_value_to_data_value};

/// Handles execution of DDL operations
pub struct DdlExecutor {
    buffer_pool: Arc<BufferPoolManager>,
    catalog: Arc<RwLock<Catalog>>,
    page_manager: PageManager,
}

impl DdlExecutor {
    pub fn new(
        buffer_pool: Arc<BufferPoolManager>,
        catalog: Arc<RwLock<Catalog>>
    ) -> Self {
        let page_manager = PageManager::new();
        DdlExecutor {
            buffer_pool,
            catalog,
            page_manager,
        }
    }

    pub fn execute_create(&self, create: CreateStatement) -> QueryResult<QueryResultSet> {
        // Use the injected catalog instance
        let catalog_instance = self.catalog.clone();
        
        // Create the table and columns outside the lock scope
        let mut columns = Vec::new();
        for col_def in &create.columns {
            let column = Column::from_column_def(col_def)?;
            columns.push(column);
        }
        
        // Create the table
        let table = Table::new(create.table_name.clone(), columns);
        
        // Acquire a write lock on the catalog to create the table
        let catalog_guard = catalog_instance.write().map_err(|e| 
            QueryError::ExecutionError(format!("Failed to acquire catalog write lock: {}", e)))?;
        catalog_guard.create_table(table)?;
        
        // If successful, return a QueryResultSet indicating success
        let mut result_set = QueryResultSet::new(vec!["status".to_string()]);
        result_set.add_row(Row::from_values(
            vec!["status".to_string()],
            vec![DataValue::Text(format!("Table '{}' created successfully.", create.table_name))]
        ));
        
        Ok(result_set)
    }

    pub fn execute_alter(&self, alter_stmt: AlterTableStatement) -> QueryResult<QueryResultSet> {
        let table_name = &alter_stmt.table_name;
        match &alter_stmt.operation {
            crate::query::parser::ast::AlterTableOperation::AddColumn(col_def) => {
                self.execute_alter_add_column(table_name, col_def)
            }
            crate::query::parser::ast::AlterTableOperation::DropColumn(col_name_to_drop) => {
                self.execute_alter_drop_column(table_name, col_name_to_drop)
            }
            crate::query::parser::ast::AlterTableOperation::RenameColumn { old_name, new_name } => {
                self.execute_alter_rename_column(table_name, old_name, new_name)
            }
            crate::query::parser::ast::AlterTableOperation::AlterColumnType { column_name, new_type } => {
                self.execute_alter_column_type(table_name, column_name, new_type)
            }
        }
    }

    fn execute_alter_add_column(&self, table_name: &str, col_def: &ColumnDef) -> QueryResult<QueryResultSet> {
        // eprintln!("VERY_UNIQUE_DEBUG_ALTER_ADD_COLUMN_AST_DEFAULT_VALUE: Table='{}', Col='{}', ASTDefaultExpr='{:?}'",
        //     table_name, col_def.name, col_def.default_value
        // );
        // std::io::stderr().flush().unwrap_or_default();

        let column = crate::catalog::column::Column::from_column_def(col_def)?;
        
        let table_schema_before_add = {
             let catalog_read_guard = self.catalog.read().map_err(|_| QueryError::CatalogError("Failed to get catalog read lock for ADD COLUMN pre-check".to_string()))?;
             catalog_read_guard.get_table(table_name) // Use .map(|t| t.clone()) instead of .cloned()
        };

        // Pre-check: If column is NOT NULL and has no DEFAULT, table must be empty.
        if !column.is_nullable() && column.get_default_ast_literal().is_none() {
            if let Some(ref table_check) = table_schema_before_add {
                let mut table_has_rows = false;
                if let Some(pid_check) = table_check.first_page_id() {
                    let page_arc_check = self.buffer_pool.fetch_page(pid_check)
                        .map_err(|e| QueryError::StorageError(format!("ADD_COL_PRECHECK: Failed to fetch page {} for emptiness check: {}", pid_check, e)))?;
                    
                    let has_records_on_first_page = {
                        let page_read_guard_check = page_arc_check.read();
                        self.page_manager.get_record_count(&page_read_guard_check).unwrap_or(0) > 0
                    };
                    
                    self.buffer_pool.unpin_page(pid_check, false)
                        .map_err(|e| QueryError::StorageError(format!("ADD_COL_PRECHECK: Failed to unpin page {} after emptiness check: {}", pid_check, e)))?;

                    if has_records_on_first_page {
                        table_has_rows = true;
                    }
                }

                if table_has_rows {
                    return Err(QueryError::ExecutionError(format!(
                        "Cannot ADD non-nullable column '{}' without a DEFAULT value to non-empty table '{}'",
                        col_def.name, // Use col_def.name directly
                        table_name
                    )));
                }
            } else {
                 // Table doesn't exist yet, so it's "empty" for this check's purpose.
                 // This case should ideally be caught by QueryError::TableNotFound if operations proceed,
                 // but for this specific check, non-existence implies it's safe to add NOT NULL without DEFAULT.
            }
        }
        
        let table_def_after_add: Table;
        let first_page_id: Option<u32>;
        {
            let catalog_write_guard = self.catalog.write().map_err(|_| QueryError::CatalogError("Failed to get catalog write lock for ADD COLUMN".to_string()))?;
            catalog_write_guard.alter_table_add_column(table_name, column.clone())?; 
            
            table_def_after_add = catalog_write_guard.get_table(table_name)
                .ok_or_else(|| QueryError::TableNotFound(format!("Table '{}' not found after alter_table_add_column.",table_name)))?.clone();
            first_page_id = table_def_after_add.first_page_id();
        } 

        if let Some(initial_pid) = first_page_id {
            if table_schema_before_add.is_some() { // Only migrate if table existed and had pages
                let determined_new_value_for_existing_rows = match &col_def.default_value { // Use default_value
                    Some(ast_expr) => { 
                        match ast_expr {
                            crate::query::parser::ast::Expression::Literal(literal_ast_val) => { 
                                let target_catalog_schema_type = helper_ast_dt_to_catalog_dt(&col_def.data_type)?;
                                ast_value_to_data_value(literal_ast_val, Some(&target_catalog_schema_type))? // Pass by ref
                            }
                            _ => {
                                 if column.is_nullable() {
                                    DataValue::Null          
                                 } else {
                                    return Err(QueryError::ExecutionError(format!(
                                        "Cannot ADD non-nullable column '{}' with complex default to existing rows without simple literal default evaluation.",
                                        column.name()
                                    )));
                                 }                                  
                            }
                        }
                    }
                    None => DataValue::Null,
                };

                let mut current_page_id_for_migration = Some(initial_pid);
                while let Some(page_id_val) = current_page_id_for_migration {
                    let page_arc_write = self.buffer_pool.fetch_page(page_id_val) 
                        .map_err(|e| QueryError::StorageError(format!("Data migration (ADD): Error fetching page {} for write: {}", page_id_val, e)))?;
                    
                    let mut page_was_modified = false;
                    let next_pid_for_loop: Option<u32>;

                    { // Scope for page_write_guard
                        let mut page_write_guard = page_arc_write.write();
                        let record_count_on_page = self.page_manager.get_record_count(&page_write_guard)
                            .map_err(|e| QueryError::StorageError(format!("Data migration (ADD): Error getting record count for page {}: {}", page_id_val, e)))?;

                        for slot_num in 0..record_count_on_page {
                            let current_rid = Rid::new(page_id_val, slot_num);
                            
                            match self.page_manager.get_record(&page_write_guard, current_rid) {
                                Ok(original_data_bytes) => {
                                    if original_data_bytes.is_empty() { continue; } 

                                    let mut deserialized_row_values: Vec<DataValue> = bincode::deserialize(&original_data_bytes)
                                        .map_err(|e| QueryError::ExecutionError(format!("Data migration (ADD): Failed to bincode::deserialize row for RID {:?}: {}", current_rid, e)))?;

                                    deserialized_row_values.push(determined_new_value_for_existing_rows.clone());

                                    let updated_data_bytes = DataValue::serialize_row(&deserialized_row_values)?;

                                    self.page_manager.update_record(&mut page_write_guard, current_rid, &updated_data_bytes)
                                        .map_err(|e| QueryError::StorageError(format!("Data migration (ADD): Failed to update record for RID {:?}: {}", current_rid, e)))?;
                                    page_was_modified = true;
                                }
                                Err(PageError::RecordNotFound) => continue, 
                                Err(e) => return Err(QueryError::StorageError(format!("Data migration (ADD): Error getting record for RID {:?}: {}", current_rid, e))),
                            }
                        }
                        next_pid_for_loop = self.page_manager.get_next_page_id(&page_write_guard)?;
                    } 

                    self.buffer_pool.unpin_page(page_id_val, page_was_modified)
                        .map_err(|e| QueryError::StorageError(format!("Data migration (ADD): Error unpinning page {}: {}", page_id_val, e)))?;
                    
                    current_page_id_for_migration = next_pid_for_loop;
                }
            }
        }
        
        let message = format!("Table '{}' altered, column '{}' added.", table_name, col_def.name);
        let mut result_set = QueryResultSet::new(vec!["status".to_string()]);
        let mut row = Row::new();
        row.set("status".to_string(), DataValue::Text(message));
        result_set.add_row(row);
        Ok(result_set)
    }

    fn execute_alter_drop_column(&self, table_name: &str, col_name_to_drop: &str) -> QueryResult<QueryResultSet> {
        let table_schema_before_drop: Table;
        let first_page_id: Option<u32>;
        let dropped_column_index: usize;

        { 
            let catalog_read_guard = self.catalog.read().map_err(|_| QueryError::CatalogError("Failed to get catalog read lock for DROP COLUMN pre-check".to_string()))?;
            let table = catalog_read_guard.get_table(table_name)
                .ok_or_else(|| QueryError::TableNotFound(table_name.to_string()))?;
            
            table_schema_before_drop = table.clone(); 
            first_page_id = table.first_page_id();
            
            dropped_column_index = table.columns().iter().position(|c| c.name() == col_name_to_drop)
                .ok_or_else(|| QueryError::ColumnNotFound(format!("Column '{}' not found in table '{}' for DROP operation.", col_name_to_drop, table_name)))?;
        }

        {
            let catalog_write_guard = self.catalog.write().map_err(|_| QueryError::CatalogError("Failed to get catalog write lock for DROP COLUMN".to_string()))?;
            catalog_write_guard.alter_table_drop_column(table_name, col_name_to_drop)?;
        }

        if let Some(initial_pid) = first_page_id {
            let table_schema_after_drop = { // This is now fetched once before the loop
                 let catalog_rg = self.catalog.read().map_err(|_| QueryError::CatalogError("Failed to get catalog read lock for serialize_row in DROP COLUMN".to_string()))?;
                 catalog_rg.get_table(table_name).ok_or_else(|| QueryError::TableNotFound(table_name.to_string()))?.clone()
            };

            let mut current_page_id_for_migration = Some(initial_pid);
            while let Some(page_id_val) = current_page_id_for_migration {
                let page_arc_write = self.buffer_pool.fetch_page(page_id_val)
                    .map_err(|e| QueryError::StorageError(format!("DROP COLUMN Data migration: Error fetching page {} for write: {}", page_id_val, e)))?;
                
                let mut page_was_modified = false;
                let next_pid_for_loop: Option<u32>;

                { // Scope for page_write_guard
                    let mut page_write_guard = page_arc_write.write();
                    let record_count_on_page = self.page_manager.get_record_count(&page_write_guard)
                        .map_err(|e| QueryError::StorageError(format!("DROP COLUMN Data migration: Error getting record count for page {}: {}", page_id_val, e)))?;

                    for slot_num in 0..record_count_on_page {
                        let current_rid = Rid::new(page_id_val, slot_num);

                        match self.page_manager.get_record(&page_write_guard, current_rid) {
                            Ok(original_data_bytes) => {
                                if original_data_bytes.is_empty() { continue; }

                                let mut deserialized_row_values: Vec<DataValue> = match bincode::deserialize(&original_data_bytes) {
                                    Ok(values) => values,
                                    Err(e) => {
                                        std::io::stderr().flush().unwrap_or_default();
                                        continue;
                                    }
                                };

                                if deserialized_row_values.len() == table_schema_before_drop.columns().len() {
                                    if dropped_column_index < deserialized_row_values.len() {
                                        deserialized_row_values.remove(dropped_column_index);
                                    } else {
                                        return Err(QueryError::ExecutionError(format!("DROP COLUMN Data migration: Dropped column index {} out of bounds for deserialized row (len {}) for RID {:?}.", dropped_column_index, deserialized_row_values.len(), current_rid)));
                                    }
                                } else {
                                    std::io::stderr().flush().unwrap_or_default();
                                    continue; 
                                }

                                let updated_data_bytes = bincode::serialize(&deserialized_row_values)
                                    .map_err(|e| QueryError::ExecutionError(format!("DROP COLUMN Data migration: Failed to serialize migrated row for RID {:?}: {}", current_rid, e)))?;

                                self.page_manager.update_record(&mut page_write_guard, current_rid, &updated_data_bytes)
                                    .map_err(|e| QueryError::StorageError(format!("DROP COLUMN Data migration: Failed to update record for RID {:?}: {}", current_rid, e)))?;
                                page_was_modified = true;
                            }
                            Err(PageError::RecordNotFound) => continue,
                            Err(e) => return Err(QueryError::StorageError(format!("DROP COLUMN Data migration: Error getting record for RID {:?}: {}", current_rid, e))),
                        }
                    }
                    next_pid_for_loop = self.page_manager.get_next_page_id(&page_write_guard)?;
                }

                self.buffer_pool.unpin_page(page_id_val, page_was_modified)
                    .map_err(|e| QueryError::ExecutionError(format!("DROP COLUMN Data migration: Error unpinning page {}: {}", page_id_val, e)))?;
                
                current_page_id_for_migration = next_pid_for_loop;
            }
        }

        let message = format!("Table '{}' altered, column '{}' dropped.", table_name, col_name_to_drop);
        let mut result_set = QueryResultSet::new(vec!["status".to_string()]);
        let mut row = Row::new();
        row.set("status".to_string(), DataValue::Text(message));
        result_set.add_row(row);
        Ok(result_set)
    }

    fn execute_alter_rename_column(&self, table_name: &str, old_name: &str, new_name: &str) -> QueryResult<QueryResultSet> {
        let catalog_write_guard = self.catalog.write().map_err(|_| QueryError::CatalogError("Failed to get catalog write lock for RENAME COLUMN".to_string()))?;
        catalog_write_guard.alter_table_rename_column(table_name, old_name, new_name)?;

        let message = format!("Table '{}' altered, column '{}' renamed to '{}'.", table_name, old_name, new_name);
        let mut result_set = QueryResultSet::new(vec!["status".to_string()]);
        let mut row = Row::new();
        row.set("status".to_string(), DataValue::Text(message));
        result_set.add_row(row);
        Ok(result_set)
    }

    fn execute_alter_column_type(
        &self, 
        table_name: &str, 
        column_name: &str, 
        new_ast_data_type: &crate::query::parser::ast::DataType
    ) -> QueryResult<QueryResultSet> {
        let new_catalog_data_type = helper_ast_dt_to_catalog_dt(new_ast_data_type)?;

        let table_schema_before_alter: Table;
        let first_page_id: Option<u32>;
        let column_to_alter_index: usize;
        // let old_catalog_data_type: crate::catalog::schema::DataType; // Not strictly needed for this version of migration logic

        { 
            let catalog_read_guard = self.catalog.read().map_err(|_| QueryError::CatalogError("Failed to get catalog read lock for ALTER COLUMN TYPE pre-check".to_string()))?;
            let table = catalog_read_guard.get_table(table_name)
                .ok_or_else(|| QueryError::TableNotFound(table_name.to_string()))?;
            
            table_schema_before_alter = table.clone();
            first_page_id = table.first_page_id();
            
            // old_catalog_data_type = table.get_column(column_name)
            //     .ok_or_else(|| QueryError::ColumnNotFound(format!("Column '{}' not found in table '{}' for ALTER COLUMN TYPE.", column_name, table_name)))?
            //     .data_type().clone();
            
            column_to_alter_index = table.columns().iter().position(|c| c.name() == column_name)
                .ok_or_else(|| QueryError::ColumnNotFound(format!("Column '{}' (to alter type) not found in table '{}'.", column_name, table_name)))?;
        }

        {
            let catalog_write_guard = self.catalog.write().map_err(|_| QueryError::CatalogError("Failed to get catalog write lock for ALTER COLUMN TYPE".to_string()))?;
            // Assuming alter_table_alter_column_type is the correct name
            catalog_write_guard.alter_table_alter_column_type(table_name, column_name, new_ast_data_type)?;
        }
        
        let table_schema_after_alter = {
            let catalog_rg = self.catalog.read().map_err(|_| QueryError::CatalogError("Failed to get catalog read lock for serialize_row in ALTER TYPE".to_string()))?;
            catalog_rg.get_table(table_name).ok_or_else(|| QueryError::TableNotFound(table_name.to_string()))?.clone()
        };

        if let Some(initial_pid) = first_page_id {
            let mut current_page_id_for_migration = Some(initial_pid);

            while let Some(page_id_val) = current_page_id_for_migration {
                let page_arc_write = self.buffer_pool.fetch_page(page_id_val)
                     .map_err(|e| QueryError::StorageError(format!("ALTER TYPE Data migration: Error fetching page {} for write: {}", page_id_val, e)))?;
                
                let mut page_was_modified = false;
                let next_pid_for_loop: Option<u32>;

                { // Scope for page_write_guard
                    let mut page_write_guard = page_arc_write.write();
                    let record_count_on_page = self.page_manager.get_record_count(&page_write_guard)
                        .map_err(|e| QueryError::StorageError(format!("ALTER TYPE Data migration: Error getting record count for page {}: {}", page_id_val, e)))?;

                    for slot_num in 0..record_count_on_page {
                        let current_rid = Rid::new(page_id_val, slot_num);

                        match self.page_manager.get_record(&page_write_guard, current_rid) {
                            Ok(original_data_bytes) => {
                                if original_data_bytes.is_empty() { continue; }

                                let mut deserialized_row_values: Vec<DataValue> = DataValue::deserialize_row(&original_data_bytes, table_schema_before_alter.columns())?;

                                if column_to_alter_index < deserialized_row_values.len() {
                                    let old_value = deserialized_row_values[column_to_alter_index].clone();
                                    
                                    // Get type names for error message
                                    let old_type_display_name = table_schema_before_alter
                                        .get_column(column_name)
                                        .ok_or_else(|| QueryError::ExecutionError(format!(
                                            "Internal error: Column '{}' not found in cached schema during ALTER TYPE error formatting.", 
                                            column_name
                                        )))?
                                        .data_type()
                                        .to_error_string();
                                    
                                    let new_type_display_name = new_catalog_data_type.to_error_string();

                                    use crate::query::executor::type_conversion::cast_to_type_if_needed;
                                    let converted_value = cast_to_type_if_needed(old_value.clone(), Some(new_ast_data_type.clone()))
                                        .map_err(|_cast_err| { // _cast_err is not directly used in the new format string
                                            QueryError::TypeError(format!(
                                                "Cannot convert {} {} to {}",
                                                old_type_display_name, // e.g. "Text"
                                                old_value.to_sql_literal_for_error(), // e.g. "'notabool'"
                                                new_type_display_name // e.g. "Boolean"
                                            ))
                                        })?;
                                    deserialized_row_values[column_to_alter_index] = converted_value;
                                } else {
                                    return Err(QueryError::ExecutionError(format!(
                                        "ALTER TYPE Data migration: Inconsistency for RID {:?}. Column index {} to alter is out of bounds for deserialized data with {} values.",
                                        current_rid, column_to_alter_index, deserialized_row_values.len()
                                    )));
                                }

                                let updated_data_bytes = DataValue::serialize_row(&deserialized_row_values)?;

                                self.page_manager.update_record(&mut page_write_guard, current_rid, &updated_data_bytes)
                                    .map_err(|e| QueryError::StorageError(format!("ALTER TYPE Data migration: Failed to update record for RID {:?}: {}", current_rid, e)))?;
                                page_was_modified = true;
                            }
                            Err(PageError::RecordNotFound) => continue,
                            Err(e) => return Err(QueryError::StorageError(format!("ALTER TYPE Data migration: Error getting record for RID {:?}: {}", current_rid, e))),
                        }
                    }
                    next_pid_for_loop = self.page_manager.get_next_page_id(&page_write_guard)?;
                }
                
                self.buffer_pool.unpin_page(page_id_val, page_was_modified)
                    .map_err(|e| QueryError::StorageError(format!("ALTER TYPE Data migration: Error unpinning page {} (dirty): {}", page_id_val, e)))?;
                
                current_page_id_for_migration = next_pid_for_loop;
            }
        }

        let message = format!("Table '{}' altered, column '{}' changed type to {:?}.", table_name, column_name, new_ast_data_type);
        let mut result_set = QueryResultSet::new(vec!["status".to_string()]);
        let mut row = Row::new();
        row.set("status".to_string(), DataValue::Text(message));
        result_set.add_row(row);
        Ok(result_set)
    }
} 