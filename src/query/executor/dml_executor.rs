// Data Manipulation Language Executor
//
// This module handles execution of DML statements like INSERT, UPDATE, DELETE.

use std::sync::Arc;
use std::sync::RwLock;
use bincode;

use crate::storage::buffer::BufferPoolManager;
use crate::storage::page::PageManager;
use crate::storage::page::PageError;
use crate::query::executor::result::{QueryResult, QueryError, QueryResultSet, DataValue, Row};
use crate::query::parser::ast::{Expression, InsertStatement, UpdateStatement, DeleteStatement};
use crate::catalog::Catalog;
use crate::catalog::Table;
use crate::transaction::concurrency::transaction_manager::TransactionManager;
use crate::transaction::IsolationLevel;
use crate::common::types::{Rid, Lsn};
use crate::query::executor::expression_eval;
use crate::query::executor::type_conversion::{convert_catalog_dt_to_ast_dt, ast_value_to_data_value};

/// Handles execution of DML operations
pub struct DmlExecutor {
    buffer_pool: Arc<BufferPoolManager>,
    catalog: Arc<RwLock<Catalog>>,
    transaction_manager: Arc<TransactionManager>,
    page_manager: PageManager,
}

impl DmlExecutor {
    pub fn new(
        buffer_pool: Arc<BufferPoolManager>,
        catalog: Arc<RwLock<Catalog>>,
        transaction_manager: Arc<TransactionManager>
    ) -> Self {
        let page_manager = PageManager::new();
        DmlExecutor {
            buffer_pool,
            catalog,
            transaction_manager,
            page_manager,
        }
    }

    /// Convert an expression to a DataValue for INSERT statements
    fn expr_to_datavalue(&self, expr: &Expression, table_def: &Table, col_name_hint: Option<&String>) -> QueryResult<DataValue> {
        // Determine target_ast_type early for use in multiple arms
        let target_ast_type: Option<crate::query::parser::ast::DataType> = col_name_hint.and_then(|name| {
            table_def.get_column(name).and_then(|col| convert_catalog_dt_to_ast_dt(col.data_type()))
        });

        match expr {
            Expression::Literal(value_ast) => {
                // The target_ast_type is now available from the function scope
                match value_ast {
                    crate::query::parser::ast::Value::Null => Ok(DataValue::Null),
                    crate::query::parser::ast::Value::Integer(i) => {
                        match target_ast_type { // Use the scoped target_ast_type
                            Some(crate::query::parser::ast::DataType::Float) => Ok(DataValue::Float(*i as f64)),
                            Some(crate::query::parser::ast::DataType::Integer) | None => Ok(DataValue::Integer(*i)),
                            Some(other_type) => Err(QueryError::ExecutionError(format!("Cannot cast integer to {:?}", other_type))),
                        }
                    },
                    crate::query::parser::ast::Value::Float(f) => {
                         match target_ast_type { // Use the scoped target_ast_type
                            Some(crate::query::parser::ast::DataType::Integer) => Ok(DataValue::Integer(*f as i64)),
                            Some(crate::query::parser::ast::DataType::Float) | None => Ok(DataValue::Float(*f)),
                            Some(other_type) => Err(QueryError::ExecutionError(format!("Cannot cast float to {:?}", other_type))),
                        }
                    },
                    crate::query::parser::ast::Value::String(s) => {
                        match target_ast_type { // Use the scoped target_ast_type
                            Some(crate::query::parser::ast::DataType::Boolean) => {
                                if s.eq_ignore_ascii_case("true") { Ok(DataValue::Boolean(true)) }
                                else if s.eq_ignore_ascii_case("false") { Ok(DataValue::Boolean(false)) }
                                else { Err(QueryError::ExecutionError(format!("Cannot cast string '{}' to Boolean", s))) }
                            }
                            Some(crate::query::parser::ast::DataType::Integer) => s.parse::<i64>()
                                .map(DataValue::Integer)
                                .map_err(|_| QueryError::ExecutionError(format!("Cannot cast string '{}' to Integer", s))),
                            Some(crate::query::parser::ast::DataType::Float) => s.parse::<f64>()
                                .map(DataValue::Float)
                                .map_err(|_| QueryError::ExecutionError(format!("Cannot cast string '{}' to Float", s))),
                            Some(crate::query::parser::ast::DataType::Text) | None => Ok(DataValue::Text(s.clone())),
                            Some(crate::query::parser::ast::DataType::Date) => Err(QueryError::ExecutionError("Date type conversion from string literal not yet implemented".to_string())),
                            Some(crate::query::parser::ast::DataType::Time) => Err(QueryError::ExecutionError("Time type conversion from string literal not yet implemented".to_string())),
                            Some(crate::query::parser::ast::DataType::Timestamp) => Err(QueryError::ExecutionError("Timestamp type conversion from string literal not yet implemented".to_string())),
                        }
                    },
                    crate::query::parser::ast::Value::Boolean(b) => {
                        match target_ast_type { // Use the scoped target_ast_type
                            Some(crate::query::parser::ast::DataType::Boolean) | None => Ok(DataValue::Boolean(*b)),
                            Some(crate::query::parser::ast::DataType::Text) => Ok(DataValue::Text(b.to_string())),
                            Some(other_type) => Err(QueryError::ExecutionError(format!("Cannot cast boolean to {:?}", other_type))),
                        }
                    }
                }
            }
            Expression::UnaryOp { op, expr: inner_expr } => {
                // Handle simple case: -NumericLiteral for INSERT VALUES
                // The target_ast_type is available from the function scope
                if let crate::query::parser::ast::UnaryOperator::Minus = op {
                    if let Expression::Literal(literal_val) = &**inner_expr {
                        match literal_val {
                            crate::query::parser::ast::Value::Integer(i) => {
                                let negated_val = -*i;
                                match target_ast_type { // Use the scoped target_ast_type
                                    Some(crate::query::parser::ast::DataType::Float) => Ok(DataValue::Float(negated_val as f64)),
                                    Some(crate::query::parser::ast::DataType::Integer) | None => Ok(DataValue::Integer(negated_val)),
                                    Some(other_type) => Err(QueryError::ExecutionError(format!("Cannot cast negative integer to {:?}", other_type))),
                                }
                            }
                            crate::query::parser::ast::Value::Float(f) => {
                                let negated_val = -*f;
                                match target_ast_type { // Use the scoped target_ast_type
                                    Some(crate::query::parser::ast::DataType::Integer) => Ok(DataValue::Integer(negated_val as i64)),
                                    Some(crate::query::parser::ast::DataType::Float) | None => Ok(DataValue::Float(negated_val)),
                                    Some(other_type) => Err(QueryError::ExecutionError(format!("Cannot cast negative float to {:?}", other_type))),
                                }
                            }
                            _ => Err(QueryError::ExecutionError("Unary minus in INSERT VALUES only supported for numeric literals.".to_string())),
                        }
                    } else {
                        Err(QueryError::ExecutionError("Unary minus in INSERT VALUES only supported for expressions evaluating to numeric literals.".to_string()))
                    }
                } else {
                    Err(QueryError::ExecutionError("Only unary minus on literals supported in INSERT VALUES for now.".to_string()))
                }
            }
            _ => Err(QueryError::ExecutionError("INSERT VALUES can only contain literals or simple negated numeric literals for now.".to_string())),
        }
    }

    pub fn execute_insert(&self, insert_stmt: InsertStatement) -> QueryResult<QueryResultSet> {
        let table_name = &insert_stmt.table_name;
        let table_id;
        let original_first_page_id: Option<u32>;
        let table_columns_for_validation: Vec<crate::catalog::Column>;

        let txn_id = self.transaction_manager.begin_transaction(IsolationLevel::ReadCommitted)
            .map_err(|e| QueryError::ExecutionError(format!("Failed to begin transaction: {}", e)))?;
        
        let txn_handle = self.transaction_manager.get_transaction(txn_id).ok_or_else(|| {
            self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
            QueryError::ExecutionError(format!("Transaction {} not found after begin.", txn_id))
        })?;

        {
            let catalog_read_guard = self.catalog.read().map_err(|e| {
                self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                QueryError::ExecutionError(format!("Failed to acquire catalog read lock: {}", e))
            })?;
            let table_def = catalog_read_guard.get_table(table_name)
                .ok_or_else(|| {
                    self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                    QueryError::TableNotFound(table_name.clone())
                })?;
            table_id = table_def.id();
            original_first_page_id = table_def.first_page_id();
            table_columns_for_validation = table_def.columns().to_vec();
        } 

        // --- Correctly restored logic for values_to_insert --- 
        let values_to_insert: Vec<DataValue>;
        let temp_table_def_for_expr_eval = Table::new(table_name.clone(), table_columns_for_validation.clone());

        if let Some(ref provided_columns) = insert_stmt.columns {
            let provided_values_count = insert_stmt.values.len();
            if provided_columns.len() != provided_values_count {
                self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                return Err(QueryError::ExecutionError(format!("Column count ({}) does not match value count ({})", provided_columns.len(), provided_values_count)));
            }
            let mut final_values = Vec::with_capacity(table_columns_for_validation.len());
            for schema_col_def in &table_columns_for_validation {
                let mut value_found_for_schema_col = false;
                if let Some(idx_in_provided) = provided_columns.iter().position(|name| name == schema_col_def.name()) {
                    let expr = &insert_stmt.values[idx_in_provided];
                    match self.expr_to_datavalue(expr, &temp_table_def_for_expr_eval, Some(&schema_col_def.name().to_string())) {
                        Ok(dv) => {
                            if let Some(target_ast_type) = convert_catalog_dt_to_ast_dt(schema_col_def.data_type()) {
                                if !dv.is_compatible_with(&target_ast_type) && dv != DataValue::Null {
                                    self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                                    return Err(QueryError::ExecutionError(format!("Type mismatch for column '{}'. Expected {:?} (AST: {:?}), got {:?}.", schema_col_def.name(), schema_col_def.data_type(), target_ast_type, dv.get_type())));
                                }
                            } else { 
                                self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                                return Err(QueryError::ExecutionError(format!("Unsupported column type {:?} for column '{}'", schema_col_def.data_type(), schema_col_def.name()))); 
                            }
                            final_values.push(dv);
                            value_found_for_schema_col = true;
                        }
                        Err(e) => { self.transaction_manager.abort_transaction(txn_id).unwrap_or_default(); return Err(e); }
                    }
                }
                if !value_found_for_schema_col { // Column was omitted from INSERT statement
                    if let Some(default_ast_literal) = schema_col_def.get_default_ast_literal() {
                        let target_catalog_schema_type = schema_col_def.data_type();
                        match ast_value_to_data_value(default_ast_literal, Some(target_catalog_schema_type)) {
                            Ok(default_dv) => {
                                final_values.push(default_dv);
                            }
                            Err(e) => {
                                self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                                return Err(QueryError::ExecutionError(format!("Failed to convert default value for column '{}': {}", schema_col_def.name(), e)));
                            }
                        }
                    } else if schema_col_def.is_nullable() {
                        final_values.push(DataValue::Null);
                    } else {
                        self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                        return Err(QueryError::ExecutionError(format!("Column '{}' is not nullable, has no default value, and no value was provided.", schema_col_def.name())));
                    }
                }
            }
            values_to_insert = final_values;
            if values_to_insert.len() != table_columns_for_validation.len() {
                self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                return Err(QueryError::ExecutionError(format!("Internal error: Mismatch between composed values ({}) and schema columns ({}).", values_to_insert.len(), table_columns_for_validation.len())));
            }
        } else {
            let provided_values_count = insert_stmt.values.len();
            if table_columns_for_validation.len() != provided_values_count { 
                self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                return Err(QueryError::ExecutionError(format!("Table '{}' has {} columns, but {} values were provided.",table_name, table_columns_for_validation.len(), provided_values_count))); 
            }
            let mut temp_values = Vec::with_capacity(provided_values_count);
            for (i, expr) in insert_stmt.values.iter().enumerate() {
                 match self.expr_to_datavalue(expr, &temp_table_def_for_expr_eval, Some(&table_columns_for_validation[i].name().to_string())) {
                    Ok(dv) => {
                        if let Some(target_ast_type) = convert_catalog_dt_to_ast_dt(table_columns_for_validation[i].data_type()) {
                             if !dv.is_compatible_with(&target_ast_type) && dv != DataValue::Null { 
                                 self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                                 return Err(QueryError::ExecutionError(format!("Type mismatch for column '{}'. Expected {:?} (AST: {:?}), got {:?}.", table_columns_for_validation[i].name(), table_columns_for_validation[i].data_type(), target_ast_type, dv.get_type()))); 
                             }
                        } else { 
                            self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                            return Err(QueryError::ExecutionError(format!("Unsupported column type {:?} for column '{}'", table_columns_for_validation[i].data_type(), table_columns_for_validation[i].name()))); 
                        }
                        temp_values.push(dv);
                    },
                    Err(e) => { self.transaction_manager.abort_transaction(txn_id).unwrap_or_default(); return Err(e); }
                }
            }
            values_to_insert = temp_values;
        }
        // --- End of logic for values_to_insert ---

        let serialized_row = bincode::serialize(&values_to_insert)
            .map_err(|e| {
                self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                QueryError::ExecutionError(format!("Failed to serialize row: {}", e))
            })?;
        
        // --- Page Operations: Fetch/Create Page, Insert Record ---
        let mut page_was_dirtied = false;
        let target_page_id: u32;
        let page_arc; // PagePtr is Arc<parking_lot::RwLock<Page>>
        let mut new_page_created_for_table = false;

        match original_first_page_id {
            Some(existing_page_id) => {
                target_page_id = existing_page_id;
                page_arc = self.buffer_pool.fetch_page(target_page_id).map_err(|e| {
                    self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                    QueryError::StorageError(format!("Failed to fetch page {}: {}", target_page_id, e))
                })?;
            }
            None => { 
                let (new_page_ptr, new_pg_id) = self.buffer_pool.new_page().map_err(|e| {
                    self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                    QueryError::StorageError(format!("Failed to create new page: {}", e))
                })?;
                target_page_id = new_pg_id;
                page_arc = new_page_ptr;
                {
                    let mut page_guard = page_arc.write(); 
                    self.page_manager.init_page(&mut page_guard);
                }
                page_was_dirtied = true; 
                new_page_created_for_table = true;
            }
        }

        let record_id: Rid;
        let final_lsn: Lsn;

        { 
            let mut page_guard = page_arc.write(); 
            record_id = self.page_manager.insert_record(&mut page_guard, &serialized_row).map_err(|e| {
                self.buffer_pool.unpin_page(target_page_id, page_was_dirtied).unwrap_or_default(); 
                self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                QueryError::ExecutionError(format!("Failed to insert record into page {}: {}", target_page_id, e))
            })?;
            page_was_dirtied = true; 

            final_lsn = txn_handle.log_insert(table_id, target_page_id, record_id.slot_num, &serialized_row).map_err(|log_err| {
                self.buffer_pool.unpin_page(target_page_id, page_was_dirtied).unwrap_or_default();
                self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                QueryError::ExecutionError(format!("Failed to log insert for table_id {}: {}", table_id, log_err))
            })?;
            
            page_guard.lsn = final_lsn;
        } 

        self.buffer_pool.unpin_page(target_page_id, page_was_dirtied).map_err(|e| {
            self.transaction_manager.abort_transaction(txn_id).unwrap_or_default(); 
            QueryError::StorageError(format!("Failed to unpin page {}: {}", target_page_id, e))
        })?;
        
        self.transaction_manager.commit_transaction(txn_id).map_err(|commit_err| {
            self.transaction_manager.abort_transaction(txn_id).unwrap_or_default(); 
            QueryError::ExecutionError(format!("Failed to commit transaction {}: {}", txn_id, commit_err))
        })?;

        if new_page_created_for_table {
            let mut catalog_w = self.catalog.write().map_err(|_e| 
                QueryError::ExecutionError(format!("Failed to acquire catalog write lock post-commit for table {}", table_name))
            )?;
            if let Some(mut_table_def) = catalog_w.get_table_mut_from_current_schema(table_name) {
                mut_table_def.set_first_page_id(target_page_id);
            } else {
                return Err(QueryError::ExecutionError(format!("Table {} not found during post-commit catalog update.", table_name)));
            }
        }

        let mut result_set = QueryResultSet::new(vec!["status".to_string()]);
        result_set.add_row(Row::from_values(
            vec!["status".to_string()], 
            vec![DataValue::Text(format!("INSERT into '{}' successful (table_id {}, page_id {}, record_id {}). {} values, {} bytes.", 
                                        table_name, table_id, target_page_id, record_id.slot_num, values_to_insert.len(), serialized_row.len()))]
        ));
        Ok(result_set)
    }

    pub fn execute_update(&self, update_stmt: UpdateStatement) -> QueryResult<QueryResultSet> {
        let table_name = update_stmt.table_name.clone();
        let catalog_guard = self.catalog.read().map_err(|e| QueryError::ExecutionError(format!("Failed to get read lock on catalog: {}", e)))?;
        let table_schema = catalog_guard.get_table(&table_name)
            .ok_or_else(|| QueryError::TableNotFound(table_name.clone()))?;
        let table_id = table_schema.id();
        let first_page_id = table_schema.first_page_id();

        if first_page_id.is_none() {
             let mut result_set = QueryResultSet::new(vec!["status".to_string(), "rows_affected".to_string()]);
             let mut row = Row::new();
             row.set("status".to_string(), DataValue::Text(format!("UPDATE completed on table '{}'. Table was empty.", table_name)));
             row.set("rows_affected".to_string(), DataValue::Integer(0));
             result_set.add_row(row);
             return Ok(result_set);
        }

        let txn_id = self.transaction_manager.begin_transaction(IsolationLevel::ReadCommitted)
            .map_err(|e| QueryError::ExecutionError(format!("Failed to begin transaction for UPDATE: {}", e)))?;
        let txn_handle = self.transaction_manager.get_transaction(txn_id).ok_or_else(|| {
            self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
            QueryError::ExecutionError(format!("Transaction {} not found after begin for UPDATE.", txn_id))
        })?;

        let mut updated_row_count = 0;
        let mut current_page_id = first_page_id;

        'page_loop: while let Some(page_id_val) = current_page_id {
            let page_rc = match self.buffer_pool.fetch_page(page_id_val) {
                 Ok(p) => p,
                 Err(e) => {
                      self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                      return Err(QueryError::StorageError(format!("Failed to fetch page {} for update: {:?}", page_id_val, e)));
                 }
             };

            let record_rids_on_page: Vec<Rid> = { 
                 let page_read_guard = page_rc.read();
                 let num_records_on_page = match self.page_manager.get_record_count(&page_read_guard) {
                     Ok(count) => count,
                     Err(e) => {
                         self.buffer_pool.unpin_page(page_id_val, false).unwrap_or_default();
                         self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                         return Err(QueryError::StorageError(format!("Failed to get record count for page {}: {:?}", page_id_val, e)));
                     }
                 };
                 (0..num_records_on_page).map(|slot_num| Rid::new(page_id_val, slot_num)).collect()
             };

             self.buffer_pool.unpin_page(page_id_val, false).map_err(|e| {
                 self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                 QueryError::StorageError(format!("Failed to unpin page {} after reading RIDs: {:?}", page_id_val, e))
             })?;

            for rid_to_check in record_rids_on_page {
                let current_record_page_rc = self.buffer_pool.fetch_page(rid_to_check.page_id)
                    .map_err(|e| {
                        self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                        QueryError::StorageError(format!("Failed to re-fetch page {} for RID {:?}: {:?}", rid_to_check.page_id, rid_to_check, e))
                    })?;
                
                let (old_record_data_bytes, current_row_obj) = { 
                    let record_page_read_guard = current_record_page_rc.read();
                    let record_data_bytes_result = self.page_manager.get_record(&record_page_read_guard, rid_to_check);

                    match record_data_bytes_result {
                        Ok(record_data_bytes) => {
                            if record_data_bytes.is_empty() {
                                self.buffer_pool.unpin_page(rid_to_check.page_id, false).unwrap_or_default();
                                continue; 
                            }
                            
                            let deserialized_values = DataValue::deserialize_row(&record_data_bytes, table_schema.columns())
                                .map_err(|e| {
                                    self.buffer_pool.unpin_page(rid_to_check.page_id, false).unwrap_or_default();
                                    self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                                    QueryError::ExecutionError(format!("Failed to deserialize row for UPDATE RID {:?}: {}", rid_to_check, e))
                                })?;
                            
                            // Convert Vec<DataValue> to Row object
                            let mut row_obj_from_vec = Row::new();
                            for (idx, col_schema) in table_schema.columns().iter().enumerate() {
                                if idx < deserialized_values.len() {
                                    row_obj_from_vec.set(col_schema.name().to_string(), deserialized_values[idx].clone());
                                }
                            }
                            (record_data_bytes.to_vec(), row_obj_from_vec)
                        }
                        Err(PageError::RecordNotFound) => {
                            self.buffer_pool.unpin_page(rid_to_check.page_id, false).unwrap_or_default();
                            continue; 
                        }
                        Err(e) => {
                            self.buffer_pool.unpin_page(rid_to_check.page_id, false).unwrap_or_default();
                            self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                            return Err(QueryError::StorageError(format!("Failed to get record bytes for UPDATE RID {:?}: {:?}", rid_to_check, e)));
                        }
                    }
                };
                
                self.buffer_pool.unpin_page(rid_to_check.page_id, false).map_err(|e| {
                    self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                    QueryError::StorageError(format!("Failed to unpin page {} after reading record for UPDATE: {:?}", rid_to_check.page_id, e))
                })?;

                let matches_where = match &update_stmt.where_clause {
                    Some(where_expr) => {
                        match expression_eval::evaluate_expression(where_expr, &current_row_obj, Some(&table_schema))? { 
                            DataValue::Boolean(b) => b,
                            DataValue::Null => false, 
                            other => {
                                 self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                                 return Err(QueryError::TypeError(format!("WHERE clause did not evaluate to boolean or NULL, got {:?}", other)));
                            }
                        }
                    }
                    None => true, 
                };

                if matches_where {
                    let mut new_row_data_values = Vec::with_capacity(table_schema.columns().len());
                    for col_schema in table_schema.columns() {
                        new_row_data_values.push(current_row_obj.get(col_schema.name()).cloned().unwrap_or(DataValue::Null));
                    }

                    for assignment in &update_stmt.assignments {
                        let col_name_to_update = &assignment.column;
                        let update_expr = &assignment.value;
                        let value_to_set = expression_eval::evaluate_expression(update_expr, &current_row_obj, Some(&table_schema))?; 
                        
                        if let Some(col_idx_to_update) = table_schema.columns().iter().position(|c| c.name() == col_name_to_update) {
                            new_row_data_values[col_idx_to_update] = value_to_set;
                        } else {
                            self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                            return Err(QueryError::ColumnNotFound(format!("Column '{}' not found in table '{}' for UPDATE of RID {:?}.", col_name_to_update, table_name, rid_to_check)));
                        }
                    }
                     
                    let new_record_data_bytes = DataValue::serialize_row(&new_row_data_values)?;

                    let page_rc_for_update = self.buffer_pool.fetch_page(rid_to_check.page_id)
                        .map_err(|e| {
                            self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                            QueryError::StorageError(format!("Failed to re-fetch page {} for UPDATE write: {:?}", rid_to_check.page_id, e))
                        })?;
                    
                    let final_lsn = txn_handle.log_update(table_id, rid_to_check.page_id, rid_to_check.slot_num, &old_record_data_bytes, &new_record_data_bytes)
                        .map_err(|log_err| {
                            self.buffer_pool.unpin_page(rid_to_check.page_id, false).unwrap_or_default(); 
                            self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                            QueryError::ExecutionError(format!("Failed to log update for RID {:?}: {}", rid_to_check, log_err))
                        })?;

                    { 
                        let mut page_write_guard = page_rc_for_update.write();
                        self.page_manager.update_record(&mut page_write_guard, rid_to_check, &new_record_data_bytes)?;
                        page_write_guard.lsn = final_lsn; 
                    } 

                    self.buffer_pool.unpin_page(rid_to_check.page_id, true).map_err(|e| { 
                        self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                        QueryError::StorageError(format!("Failed to unpin page {} after UPDATE: {:?}", rid_to_check.page_id, e))
                    })?;
                    updated_row_count += 1;
                }
            } 

            let page_rc_for_next = self.buffer_pool.fetch_page(page_id_val)
                 .map_err(|e| QueryError::StorageError(format!("Failed to fetch page {} for next page ID: {:?}", page_id_val, e)))?;
            let page_read_guard = page_rc_for_next.read(); 
            current_page_id = self.page_manager.get_next_page_id(&page_read_guard)?;
            self.buffer_pool.unpin_page(page_id_val, false)
                 .map_err(|e| QueryError::StorageError(format!("Failed to unpin page {} after getting next ID: {:?}", page_id_val, e)))?;

        } 

        self.transaction_manager.commit_transaction(txn_id)
            .map_err(|e| QueryError::ExecutionError(format!("Failed to commit transaction {} for UPDATE: {}", txn_id, e)))?;

        let mut result_set = QueryResultSet::new(vec!["status".to_string(), "rows_affected".to_string()]);
        let mut row = Row::new();
        row.set("status".to_string(), DataValue::Text(format!("UPDATE completed on table '{}'", table_name)));
        row.set("rows_affected".to_string(), DataValue::Integer(updated_row_count as i64));
        result_set.add_row(row);
        Ok(result_set)
    }

    pub fn execute_delete(&self, delete_stmt: DeleteStatement) -> QueryResult<QueryResultSet> {
        let table_name = delete_stmt.table_name.clone();
        let catalog_guard = self.catalog.read().map_err(|e| QueryError::ExecutionError(format!("Failed to get read lock on catalog: {}", e)))?;
        let table_schema = catalog_guard.get_table(&table_name)
            .ok_or_else(|| QueryError::TableNotFound(table_name.clone()))?;
        let table_id = table_schema.id();
        let first_page_id = table_schema.first_page_id();
        if first_page_id.is_none() {
            let mut result_set = QueryResultSet::new(vec!["status".to_string(), "rows_affected".to_string()]);
            let mut row = Row::new();
            row.set("status".to_string(), DataValue::Text(format!("DELETE completed on table '{}'", table_name)));
            row.set("rows_affected".to_string(), DataValue::Integer(0));
            result_set.add_row(row);
            return Ok(result_set);
        }
        let mut current_page_id = first_page_id;
        let mut deleted_row_count = 0;
        let txn_id = self.transaction_manager.begin_transaction(IsolationLevel::ReadCommitted)
            .map_err(|e| QueryError::ExecutionError(format!("Failed to begin transaction for DELETE: {}", e)))?;
        let txn_handle = self.transaction_manager.get_transaction(txn_id).ok_or_else(|| {
            self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
            QueryError::ExecutionError(format!("Transaction {} not found after begin for DELETE.", txn_id))
        })?;
        'page_loop: while let Some(page_id_val) = current_page_id {
            let page_rc = self.buffer_pool.fetch_page(page_id_val)
                .map_err(|e| {
                    self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                    QueryError::StorageError(format!("Failed to fetch page {} for delete: {:?}", page_id_val, e))
                })?;
            let record_rids_on_page: Vec<Rid> = {
                let page_read_guard = page_rc.read();
                let num_records_on_page = self.page_manager.get_record_count(&page_read_guard)
                    .map_err(|e| {
                        self.buffer_pool.unpin_page(page_id_val, false).unwrap_or_default();
                        self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                        QueryError::StorageError(format!("Failed to get record count for page {}: {:?}", page_id_val, e))
                    })?;
                (0..num_records_on_page).map(|slot_num| Rid::new(page_id_val, slot_num)).collect()
            };
            self.buffer_pool.unpin_page(page_id_val, false).map_err(|e| {
                self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                QueryError::StorageError(format!("Failed to unpin page {} after reading RIDs: {:?}", page_id_val, e))
            })?;
            for rid_to_check in record_rids_on_page {
                let current_record_page_rc = self.buffer_pool.fetch_page(rid_to_check.page_id)
                    .map_err(|e| {
                        self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                        QueryError::StorageError(format!("Failed to re-fetch page {} for RID {:?}: {:?}", rid_to_check.page_id, rid_to_check, e))
                    })?;
                let (old_record_data_bytes, current_row_obj) = { // Renamed current_row to current_row_obj for clarity
                    let record_page_read_guard = current_record_page_rc.read();
                    let record_data_bytes = match self.page_manager.get_record(&record_page_read_guard, rid_to_check) {
                        Ok(bytes) => bytes,
                        Err(_) => {
                            self.buffer_pool.unpin_page(rid_to_check.page_id, false).unwrap_or_default();
                            continue;
                        }
                    };
                    if record_data_bytes.is_empty() {
                        self.buffer_pool.unpin_page(rid_to_check.page_id, false).unwrap_or_default();
                        continue;
                    }
                    let deserialized_values: Vec<DataValue> = match bincode::deserialize(&record_data_bytes) {
                        Ok(values) => values,
                        Err(_) => {
                            self.buffer_pool.unpin_page(rid_to_check.page_id, false).unwrap_or_default();
                            continue;
                        }
                    };
                    let mut row_obj_built = Row::new(); // Renamed row_obj to row_obj_built
                    for (col_idx, col_schema) in table_schema.columns().iter().enumerate() {
                        row_obj_built.set(col_schema.name().to_string(), deserialized_values.get(col_idx).cloned().unwrap_or(DataValue::Null));
                    }
                    (record_data_bytes, row_obj_built)
                };
                self.buffer_pool.unpin_page(rid_to_check.page_id, false).map_err(|e| {
                    self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                    QueryError::StorageError(format!("Failed to unpin page {} after reading record data for RID {:?}: {:?}", rid_to_check.page_id, rid_to_check, e))
                })?;
                let mut matches_where = true;
                if let Some(where_expr) = &delete_stmt.where_clause {
                    let where_eval_result = expression_eval::evaluate_expression(where_expr, &current_row_obj, Some(&table_schema))?; // Use current_row_obj
                    if let DataValue::Boolean(val) = where_eval_result {
                        matches_where = val;
                    } else {
                        matches_where = false;
                    }
                }
                if matches_where {
                    let page_for_delete_rc = self.buffer_pool.fetch_page(rid_to_check.page_id)
                        .map_err(|e| {
                            self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                            QueryError::StorageError(format!("Failed to fetch page {} for final delete of RID {:?}: {:?}", rid_to_check.page_id, rid_to_check, e))
                        })?;
                    let final_lsn = txn_handle.log_delete(table_id, rid_to_check.page_id, rid_to_check.slot_num, &old_record_data_bytes)
                        .map_err(|log_err| {
                            self.buffer_pool.unpin_page(rid_to_check.page_id, false).unwrap_or_default();
                            self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                            QueryError::ExecutionError(format!("Failed to log delete for RID {:?}: {}", rid_to_check, log_err))
                        })?;
                    {
                        let mut page_write_guard = page_for_delete_rc.write();
                        self.page_manager.delete_record(&mut page_write_guard, rid_to_check)
                            .map_err(|e| {
                                self.buffer_pool.unpin_page(rid_to_check.page_id, false).unwrap_or_default();
                                self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                                QueryError::StorageError(format!("Failed to delete record on page for RID {:?}: {:?}", rid_to_check, e))
                            })?;
                        page_write_guard.lsn = final_lsn;
                    }
                    self.buffer_pool.unpin_page(rid_to_check.page_id, true).map_err(|e| {
                        self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                        QueryError::StorageError(format!("Failed to unpin page {} (dirty) after delete of RID {:?}: {:?}", rid_to_check.page_id, rid_to_check, e))
                    })?;
                    deleted_row_count += 1;
                }
            }
            let page_rc_for_next_pid = self.buffer_pool.fetch_page(page_id_val)
                .map_err(|e| {
                    self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                    QueryError::StorageError(format!("Failed to fetch page {} to get next page ID: {:?}", page_id_val, e))
                })?;
            {
                let page_read_guard = page_rc_for_next_pid.read();
                current_page_id = self.page_manager.get_next_page_id(&page_read_guard)?;
            }
            self.buffer_pool.unpin_page(page_id_val, false).map_err(|e| {
                self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                QueryError::StorageError(format!("Failed to unpin page {} after getting next page ID: {:?}", page_id_val, e))
            })?;
            if current_page_id.is_none() {
                break 'page_loop;
            }
        }
        self.transaction_manager.commit_transaction(txn_id).map_err(|commit_err| {
            self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
            QueryError::ExecutionError(format!("Failed to commit transaction {} for DELETE: {}", txn_id, commit_err))
        })?;
        let mut result_set = QueryResultSet::new(vec!["status".to_string(), "rows_affected".to_string()]);
        let mut row = Row::new();
        row.set("status".to_string(), DataValue::Text(format!("DELETE completed on table '{}'", table_name)));
        row.set("rows_affected".to_string(), DataValue::Integer(deleted_row_count as i64));
        result_set.add_row(row);
        Ok(result_set)
    }
} 