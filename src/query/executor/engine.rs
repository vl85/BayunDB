// Query Execution Engine Implementation
//
// This module implements the engine for executing SQL queries.

use std::sync::Arc;
use std::sync::RwLock;
use std::io::Write; // Added for stderr.flush()

use crate::storage::buffer::BufferPoolManager;
use crate::storage::page::PageManager;
use crate::storage::page::PageError;
use crate::query::executor::result::{QueryResult, QueryError, QueryResultSet, DataValue, Row};
use crate::query::parser::ast::{Statement, SelectStatement, Expression, CreateStatement, InsertStatement, UpdateStatement, DeleteStatement};
use crate::query::parser::parse;
use crate::catalog::Catalog;
use crate::catalog::{Table, Column};
use bincode;
use crate::transaction::concurrency::transaction_manager::TransactionManager;
use crate::transaction::IsolationLevel;
use crate::common::types::{Rid, Lsn};
use crate::common::types::PagePtr;
use crate::query::executor::operators::Operator;
use crate::query::planner::physical_plan::PhysicalPlan;
use crate::query::planner::Planner;
use crate::query::planner::operator_builder::OperatorBuilder;
use crate::query::executor::expression_eval;

// Helper function to convert ast::DataType to catalog::schema::DataType (Workaround)
fn helper_ast_dt_to_catalog_dt(ast_dt: &crate::query::parser::ast::DataType) -> QueryResult<crate::catalog::schema::DataType> {
    crate::catalog::schema::DataType::from_str(&ast_dt.to_string())
        .map_err(|_| QueryError::TypeError(format!("Cannot map AST type {:?} to catalog type", ast_dt)))
}

// Original helper function (ensure it's still here and correct)
fn convert_catalog_dt_to_ast_dt(catalog_dt: &crate::catalog::schema::DataType) -> Option<crate::query::parser::ast::DataType> {
    match catalog_dt {
        crate::catalog::schema::DataType::Integer => Some(crate::query::parser::ast::DataType::Integer),
        crate::catalog::schema::DataType::Float => Some(crate::query::parser::ast::DataType::Float),
        crate::catalog::schema::DataType::Text => Some(crate::query::parser::ast::DataType::Text),
        crate::catalog::schema::DataType::Boolean => Some(crate::query::parser::ast::DataType::Boolean),
        crate::catalog::schema::DataType::Date => Some(crate::query::parser::ast::DataType::Date),
        crate::catalog::schema::DataType::Timestamp => Some(crate::query::parser::ast::DataType::Timestamp),
        // Assuming catalog doesn't have Time yet, or it maps from Text or another type.
        // If catalog::schema::DataType gets a Time variant, this should map to it.
        crate::catalog::schema::DataType::Blob => None, 
    }
}

pub struct ExecutionEngine {
    buffer_pool: Arc<BufferPoolManager>,
    catalog: Arc<RwLock<Catalog>>,
    transaction_manager: Arc<TransactionManager>,
    planner: Planner,
    page_manager: PageManager,
    operator_builder: OperatorBuilder,
}

impl ExecutionEngine {
    pub fn new(
        buffer_pool: Arc<BufferPoolManager>, 
        catalog: Arc<RwLock<Catalog>>, 
        transaction_manager: Arc<TransactionManager>
    ) -> Self {
        // Corrected Planner::new call with both arguments
        let planner = Planner::new(buffer_pool.clone(), catalog.clone()); 
        let page_manager = PageManager::new(); // Corrected call with no arguments
        let operator_builder = OperatorBuilder::new(buffer_pool.clone(), catalog.clone());
        ExecutionEngine {
            buffer_pool,
            catalog,
            transaction_manager,
            planner,
            page_manager,
            operator_builder,
        }
    }
    
    fn plan_is_aggregate(plan: &PhysicalPlan) -> bool {
        matches!(plan, PhysicalPlan::HashAggregate { .. }) 
    }
    
    pub fn execute(&self, statement: Statement) -> QueryResult<QueryResultSet> {
        match statement {
            Statement::Select(select) => self.execute_select(select),
            Statement::Insert(insert) => self.execute_insert(insert),
            Statement::Update(update) => self.execute_update(update),
            Statement::Delete(delete) => self.execute_delete(delete),
            Statement::Create(create) => self.execute_create(create),
            Statement::Alter(alter_stmt) => {
                if !self.transaction_manager.get_active_transaction_ids().is_empty() {
                    return Err(QueryError::ExecutionError("DDL operations are blocked until all transactions complete (no active transactions allowed during schema changes)".to_string()));
                }
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
        }
    }
    
    fn execute_select(&self, select: SelectStatement) -> QueryResult<QueryResultSet> {
        // Create a logical plan for the SELECT statement
        let logical_plan = self.planner.create_logical_plan(&Statement::Select(select))?;
        
        // Create a physical plan from the logical plan
        let physical_plan = self.planner.create_physical_plan(&logical_plan)?;
        
        // Execute the physical plan
        self.execute_physical_plan(&physical_plan)
    }
    
    fn execute_physical_plan(&self, plan: &PhysicalPlan) -> QueryResult<QueryResultSet> {
        // Build operator tree from physical plan
        let root_op = self.operator_builder.build_operator_tree(plan)?;
        
        // Initialize the operator
        {
            let mut op = root_op.lock().map_err(|e| {
                QueryError::ExecutionError(format!("Failed to lock root operator: {}", e))
            })?;
            op.init()?;
        }
        
        // Helper to extract a string name from an Expression, typically a column or its alias
        fn extract_column_name_from_expression(expr: &Expression) -> String {
            match expr {
                Expression::Column(col_ref) => col_ref.name.clone(),
                // TODO: Handle Expression::Alias if/when it's added to AST for SELECT item aliasing:
                // Expression::Alias { expr: _, name } => name.clone(),
                Expression::Aggregate { function, arg } => {
                    let func_name = match function {
                        crate::query::parser::ast::AggregateFunction::Count => "COUNT",
                        crate::query::parser::ast::AggregateFunction::Sum => "SUM",
                        crate::query::parser::ast::AggregateFunction::Avg => "AVG",
                        crate::query::parser::ast::AggregateFunction::Min => "MIN",
                        crate::query::parser::ast::AggregateFunction::Max => "MAX",
                    };
                    if let Some(arg_expr) = arg {
                        format!("{}({})", func_name, extract_column_name_from_expression(arg_expr))
                    } else {
                        format!("{} (*)", func_name) // COUNT(*) case
                    }
                }
                // For other complex expressions, generate a placeholder or a string representation
                // This might need to be more sophisticated depending on how complex expressions are named.
                _ => expr.to_string(), // Assuming Expression has a reasonable to_string() for now
            }
        }

        // Extract expected column names from the plan
        let expected_columns: Vec<String> = match plan {
            PhysicalPlan::HashAggregate { aggregate_select_expressions, .. } => { // Removed group_by from pattern
                // The aggregate_select_expressions already contain all output columns (group keys and aggregates)
                // in the correct order and with their final output names (aliases).
                aggregate_select_expressions.iter().map(|se| se.output_name.clone()).collect()
            }
            PhysicalPlan::Project { columns: expr_columns, .. } => {
                expr_columns.clone()
            }
            PhysicalPlan::SeqScan { table_name, alias, .. } => {
                let catalog_guard = self.catalog.read().map_err(|_| QueryError::ExecutionError("Failed to get catalog read lock for SeqScan columns".to_string()))?;
                let table_schema = catalog_guard.get_table(table_name)
                    .ok_or_else(|| QueryError::TableNotFound(table_name.clone()))?;
                table_schema.columns().iter().map(|col| col.name().to_string()).collect()
            }
            PhysicalPlan::Filter { input, .. } | PhysicalPlan::Sort { input, .. } => {
                // For Filter and Sort, the output columns are determined by their input plan.
                // We need to recursively (or iteratively) find the base that defines columns.
                // This is a simplified way to handle common cases. A more robust solution
                // would involve each PhysicalPlan node having a `schema()` method.
                // For now, inspect common inputs that define columns:
                match &**input {
                    PhysicalPlan::Project { columns: expr_columns, .. } => expr_columns.clone(),
                    PhysicalPlan::SeqScan { table_name, .. } => {
                        let catalog_guard = self.catalog.read().map_err(|_| QueryError::ExecutionError("Failed to get catalog read lock for input Scan columns of current plan".to_string()))?;
                        let table_schema = catalog_guard.get_table(table_name)
                            .ok_or_else(|| QueryError::TableNotFound(table_name.clone()))?;
                        table_schema.columns().iter().map(|col| col.name().to_string()).collect()
                    }
                    PhysicalPlan::HashAggregate { aggregate_select_expressions, .. } => { // Removed group_by from pattern
                        // Consistent with the above HashAggregate arm
                        aggregate_select_expressions.iter().map(|se| se.output_name.clone()).collect()
                    }
                    // If the input is another Filter or Sort, this logic might need to be deeper.
                    // For now, if it's not one of the above, it might lead to empty columns.
                    _ => {
                        // Fallback: try to execute the input plan just to get its schema.
                        // This is inefficient and potentially problematic.
                        // A better approach is that all plans carry their schema.
                        // For now, if the direct input isn't a Project/Scan/Aggregate, we might still get empty.
                        // This indicates a need for `PhysicalPlan::schema(&self)`
                        Vec::new()
                    }
                }
            }
            // For Joins, if they are the root (unlikely without a projection), this is complex.
            // The schema depends on the selected columns from the join.
            // For now, this will default to Vec::new(), meaning row columns are used as is.
            // PhysicalPlan::NestedLoopJoin { .. } |
            // PhysicalPlan::HashJoin { .. } => {
            //    Vec::new()
            // }
            _ => {
                Vec::new() // Default for other plan types (like CreateTable, Insert, or unhandled Joins at root)
            }
        };
        
        // Create result set with expected columns
        let mut result_set = QueryResultSet::new(expected_columns.clone());
        
        // Get rows from the operator
        let mut op = root_op.lock().map_err(|e| {
            QueryError::ExecutionError(format!("Failed to lock root operator: {}", e))
        })?;
        
        let mut is_first_row = true;
        while let Some(row) = op.next()? {
            if is_first_row && expected_columns.is_empty() {
                // If columns were not known from plan, infer from first row.
                // This is a fallback. `expected_columns` should ideally always be set.
                // The QueryResultSet is already created with (empty) expected_columns.
                // To set them now, we'd need a mutable method on QueryResultSet or reconstruct it.
                // For now, this means if expected_columns is empty, they remain empty in the result.
                // This line was: result_set.set_columns(row.columns().to_vec()); // This method doesn't exist
                // To fix properly, QueryResultSet might need a `pub(crate) fn set_internal_columns`
                // or the logic for `expected_columns` needs to be more robust to avoid being empty.
                // For now, removing the problematic call.
                // If `row.columns()` is needed, `result_set.columns` should be updated *before* adding rows.
            }
            is_first_row = false;

            // Ensure column names consistency for aggregates or if expected_columns were defined
            if !expected_columns.is_empty() {
                let mut remapped_row = Row::new();
                let all_expected_found = true;
                for expected_col_name in &expected_columns {
                    let value_opt = row.get(expected_col_name);

                    if let Some(value) = value_opt {
                        remapped_row.set(expected_col_name.to_string(), value.clone());
                    } else {
                        let mut found_qualified = false;
                        if let PhysicalPlan::SeqScan { alias, .. } = plan { 
                            let qualified_name = format!("{}.{}", alias.as_ref().unwrap_or(&String::new()), expected_col_name);
                            if let Some(value) = row.get(&qualified_name) {
                                remapped_row.set(expected_col_name.to_string(), value.clone());
                                found_qualified = true;
                            }
                        } else if let PhysicalPlan::Project { input, .. } = plan { 
                            if let PhysicalPlan::SeqScan { alias, table_name, ..} = &**input { 
                                 let qualified_name_from_scan = format!("{}.{}", alias.as_ref().unwrap_or(table_name), expected_col_name);
                                 if let Some(value) = row.get(&qualified_name_from_scan) {
                                     remapped_row.set(expected_col_name.to_string(), value.clone());
                                     found_qualified = true;
                                 }
                            }
                        }

                        if !found_qualified {
                             remapped_row.set(expected_col_name.to_string(), DataValue::Null);
                        }
                    }
                }

                // If not all columns were found by direct/qualified name, fall back to the existing aggregate remapping logic.
                // This aggregate remapping logic might be too specific if `expected_columns` are not just aggregates.
                if !all_expected_found && Self::plan_is_aggregate(plan) { // plan_is_aggregate is a conceptual helper
                    for (i, column) in expected_columns.iter().enumerate() {
                        if remapped_row.get(column).is_none() || matches!(remapped_row.get(column), Some(DataValue::Null)) { // Check if we already filled it or if it's Null
                            // For COUNT(*), check if there's a COUNT(*) column with a different format
                            if column == "COUNT(*)" {
                                let row_values_from_op = row.values(); // original row from operator
                                if i < row_values_from_op.len() {
                                    let count_value = row_values_from_op[i].clone();
                                    remapped_row.set(column.to_string(), count_value);
                                }
                            }
                            // For other aggregate columns, try to find a matching column by function type
                            else if column.starts_with("COUNT(") ||
                                    column.starts_with("SUM(") ||
                                    column.starts_with("AVG(") ||
                                    column.starts_with("MIN(") ||
                                    column.starts_with("MAX(") {
                                let column_values_from_op: Vec<(String, DataValue)> = row.values_with_names()
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect();
                                for (col_name_from_op, value_from_op) in column_values_from_op {
                                    if col_name_from_op.starts_with(&column[..4]) { // Compare first 4 chars (e.g., "SUM(" vs "SUM(")
                                        remapped_row.set(column.to_string(), value_from_op);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                 result_set.add_row(remapped_row);
            } else {
                // If expected_columns was empty, add row as-is.
                result_set.add_row(row);
            }
        }
        
        // Close the operator
        op.close()?;
        
        Ok(result_set)
    }
    
    fn execute_create(&self, create: CreateStatement) -> QueryResult<QueryResultSet> {
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

    fn execute_update(&self, update_stmt: UpdateStatement) -> QueryResult<QueryResultSet> {
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

    fn execute_delete(&self, delete_stmt: DeleteStatement) -> QueryResult<QueryResultSet> {
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

    pub fn execute_query(&self, query: &str) -> QueryResult<QueryResultSet> {
        let statement = parse(query)?;
        match &statement {
            Statement::Insert(insert_stmt) => {
                // For INSERT, we directly call execute_insert for now
                // This bypasses the planner for INSERT as it's simpler
                // TODO: Eventually, INSERT might also go through a planner for features like INSERT FROM SELECT
                return self.execute_insert(insert_stmt.clone());
            }
            Statement::Create(create_stmt) => {
                // Similar to INSERT, CREATE is directly executed.
                return self.execute_create(create_stmt.clone());
            }
            Statement::Update(update_stmt) => {
                // Bypass planner for UPDATE for now
                return self.execute_update(update_stmt.clone());
            }
            Statement::Delete(delete_stmt) => {
                // Bypass planner for DELETE for now
                return self.execute_delete(delete_stmt.clone());
            }
            Statement::Alter(_) => {
                // ALTER statements should be executed directly, not via planner and dummy operator
                return self.execute(statement); // Pass the owned statement
            }
            // Other statements will go through the planner
            _ => {}
        }

        // Create a logical plan
        let logical_plan = self.planner.create_logical_plan(&statement)?;
        
        // Create a physical plan from the logical plan
        let physical_plan = self.planner.create_physical_plan(&logical_plan)?;
        
        // Execute the physical plan
        self.execute_physical_plan(&physical_plan)
    }

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
                            Some(crate::query::parser::ast::DataType::Time) => Err(QueryError::ExecutionError("Time type conversion from string literal not yet implemented".to_string())), // Added this arm
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

    fn execute_insert(&self, insert_stmt: InsertStatement) -> QueryResult<QueryResultSet> {
        let table_name = &insert_stmt.table_name;
        let table_id;
        let original_first_page_id: Option<u32>;
        let table_columns_for_validation: Vec<Column>;

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
                        match self.ast_value_to_data_value(default_ast_literal, Some(target_catalog_schema_type)) {
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
        // --- End of restored logic for values_to_insert ---

        let serialized_row = bincode::serialize(&values_to_insert)
            .map_err(|e| {
                self.transaction_manager.abort_transaction(txn_id).unwrap_or_default();
                QueryError::ExecutionError(format!("Failed to serialize row: {}", e))
            })?;
        
        // --- Page Operations: Fetch/Create Page, Insert Record ---
        let mut page_was_dirtied = false;
        let target_page_id: u32;
        let page_arc: PagePtr; // PagePtr is Arc<parking_lot::RwLock<Page>>
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

    // Helper function to cast a DataValue string to a target DataType if needed
    fn cast_to_type_if_needed(&self, value: DataValue, target_ast_type: Option<crate::query::parser::ast::DataType>) -> QueryResult<DataValue> {
        if target_ast_type.is_none() {
            return Ok(value);
        }

        match value {
            DataValue::Null => Ok(DataValue::Null), // Null casts to Null
            DataValue::Text(s) => {
                // Attempt to parse s into the target type
                if let Some(target_type) = target_ast_type {
                    match target_type {
                        crate::query::parser::ast::DataType::Integer => s.parse::<i64>().map(DataValue::Integer).map_err(|e| QueryError::ExecutionError(format!("Invalid cast from text '{}' to integer: {}", s, e))),
                        crate::query::parser::ast::DataType::Float => s.parse::<f64>().map(DataValue::Float).map_err(|e| QueryError::ExecutionError(format!("Invalid cast from text '{}' to float: {}", s, e))),
                        crate::query::parser::ast::DataType::Boolean => {
                            match s.to_lowercase().as_str() {
                                "true" | "t" | "1" => Ok(DataValue::Boolean(true)),
                                "false" | "f" | "0" => Ok(DataValue::Boolean(false)),
                                _ => Err(QueryError::ExecutionError(format!("Invalid cast from text '{}' to boolean", s)))
                            }
                        },
                        crate::query::parser::ast::DataType::Date => {
                            // Basic YYYY-MM-DD check. Real parsing is more complex.
                            if s.len() == 10 && s.chars().nth(4) == Some('-') && s.chars().nth(7) == Some('-') {
                                Ok(DataValue::Date(s))
                            } else {
                                Err(QueryError::ExecutionError(format!("Invalid cast from text '{}' to date. Expected YYYY-MM-DD.", s)))
                            }
                        },
                        crate::query::parser::ast::DataType::Time => {
                             Err(QueryError::ExecutionError("Casting text to Time not yet implemented".to_string()))
                        },
                        crate::query::parser::ast::DataType::Timestamp => {
                            // Basic check. Real parsing is more complex.
                            if s.len() >= 19  {
                                Ok(DataValue::Timestamp(s))
                            } else {
                                Err(QueryError::ExecutionError(format!("Invalid cast from text '{}' to timestamp.",s)))
                            }
                        },
                        crate::query::parser::ast::DataType::Text => Ok(DataValue::Text(s)), // Text to Text is a no-op
                    }
                } else {
                    Ok(DataValue::Text(s)) // No target type, return as is
                }
            }
            DataValue::Integer(i) => {
                 match target_ast_type {
                    Some(crate::query::parser::ast::DataType::Float) => Ok(DataValue::Float(i as f64)),
                    Some(crate::query::parser::ast::DataType::Text) => Ok(DataValue::Text(i.to_string())),
                    Some(crate::query::parser::ast::DataType::Integer) => Ok(DataValue::Integer(i)), // No-op
                    Some(crate::query::parser::ast::DataType::Boolean) => Ok(DataValue::Boolean(i != 0)), // 0 is false, others true
                    Some(crate::query::parser::ast::DataType::Time) => Err(QueryError::ExecutionError("Casting Integer to Time not supported".to_string())),
                    Some(_) => Err(QueryError::ExecutionError(format!("Unsupported cast from Integer to {:?}", target_ast_type.unwrap()))),
                    None => Ok(DataValue::Integer(i)),
                }
            }
            DataValue::Float(f) => {
                match target_ast_type { 
                    Some(crate::query::parser::ast::DataType::Integer) => Ok(DataValue::Integer(f.trunc() as i64)), // Truncate
                    Some(crate::query::parser::ast::DataType::Text) => Ok(DataValue::Text(f.to_string())),
                    Some(crate::query::parser::ast::DataType::Float) => Ok(DataValue::Float(f)), // No-op
                    Some(crate::query::parser::ast::DataType::Time) => Err(QueryError::ExecutionError("Casting Float to Time not supported".to_string())),
                    Some(_) => Err(QueryError::ExecutionError(format!("Unsupported cast from Float to {:?}", target_ast_type.unwrap()))),
                    None => Ok(DataValue::Float(f)),
                }
            }
            DataValue::Boolean(b) => {
                 match target_ast_type {
                    Some(crate::query::parser::ast::DataType::Text) => Ok(DataValue::Text(b.to_string())),
                    Some(crate::query::parser::ast::DataType::Integer) => Ok(DataValue::Integer(if b {1} else {0})),
                    Some(crate::query::parser::ast::DataType::Boolean) => Ok(DataValue::Boolean(b)), // No-op
                    Some(crate::query::parser::ast::DataType::Time) => Err(QueryError::ExecutionError("Casting Boolean to Time not supported".to_string())),
                    Some(_) => Err(QueryError::ExecutionError(format!("Unsupported cast from Boolean to {:?}", target_ast_type.unwrap()))),
                    None => Ok(DataValue::Boolean(b)),
                }
            }
            DataValue::Date(d) => { // Assuming Date is stored as String internally in DataValue
                match target_ast_type {
                    Some(crate::query::parser::ast::DataType::Text) => Ok(DataValue::Text(d)),
                    Some(crate::query::parser::ast::DataType::Timestamp) => Ok(DataValue::Timestamp(format!("{} 00:00:00", d))), // Basic conversion
                    Some(crate::query::parser::ast::DataType::Date) => Ok(DataValue::Date(d)), // No-op
                    Some(crate::query::parser::ast::DataType::Time) => Err(QueryError::ExecutionError("Casting Date to Time not supported".to_string())),
                    Some(_) => Err(QueryError::ExecutionError(format!("Unsupported cast from Date to {:?}", target_ast_type.unwrap()))),
                    None => Ok(DataValue::Date(d)),
                }
            }
            DataValue::Timestamp(ts) => { // Assuming Timestamp is stored as String internally
                 match target_ast_type {
                    Some(crate::query::parser::ast::DataType::Text) => Ok(DataValue::Text(ts)),
                    Some(crate::query::parser::ast::DataType::Date) => Ok(DataValue::Date(ts.split(' ').next().unwrap_or_default().to_string())), // Basic conversion
                    Some(crate::query::parser::ast::DataType::Timestamp) => Ok(DataValue::Timestamp(ts)), // No-op
                    Some(crate::query::parser::ast::DataType::Time) => Err(QueryError::ExecutionError("Casting Timestamp to Time not supported".to_string())),
                    Some(_) => Err(QueryError::ExecutionError(format!("Unsupported cast from Timestamp to {:?}", target_ast_type.unwrap()))),
                    None => Ok(DataValue::Timestamp(ts)),
                }
            }
            DataValue::Blob(_) => { // Blob casting (e.g., to text if hex encoded, or error)
                match target_ast_type {
                    // Example: cast blob to text if it's a common request, might involve hex encoding
                    // Some(crate::query::parser::ast::DataType::Text) => Ok(DataValue::Text(hex::encode(data))), 
                    Some(crate::query::parser::ast::DataType::Time) => Err(QueryError::ExecutionError("Casting Blob to Time not supported".to_string())),
                    Some(_) => Err(QueryError::ExecutionError("Casting from Blob not supported for this type".to_string())),
                    None => Ok(value), // No cast requested, return as is
                }
            }
        }
    }

    /// Converts an AST Value into a DataValue, attempting to use schema for type hinting strings
    fn ast_value_to_data_value(&self, ast_value: &crate::query::parser::ast::Value, target_schema_type: Option<&crate::catalog::schema::DataType>) -> QueryResult<DataValue> {
        let data_value = match ast_value {
            crate::query::parser::ast::Value::Integer(i) => DataValue::Integer(*i),
            crate::query::parser::ast::Value::Float(f) => DataValue::Float(*f),
            crate::query::parser::ast::Value::String(s) => DataValue::Text(s.clone()),
            crate::query::parser::ast::Value::Boolean(b) => DataValue::Boolean(*b),
            crate::query::parser::ast::Value::Null => DataValue::Null,
        };

        if let crate::query::parser::ast::Value::String(_) = ast_value {
            if let Some(schema_dt) = target_schema_type {
                let target_ast_type = convert_catalog_dt_to_ast_dt(schema_dt);
                return self.cast_to_type_if_needed(data_value, target_ast_type);
            }
        }
        Ok(data_value)
    }

    fn execute_alter_add_column(&self, table_name: &str, col_def: &crate::query::parser::ast::ColumnDef) -> QueryResult<QueryResultSet> {
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
                            Expression::Literal(literal_ast_val) => { 
                                let target_catalog_schema_type = helper_ast_dt_to_catalog_dt(&col_def.data_type)?;
                                self.ast_value_to_data_value(literal_ast_val, Some(&target_catalog_schema_type))? // Pass by ref
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

                                    let converted_value = self.cast_to_type_if_needed(old_value.clone(), Some(new_ast_data_type.clone()))
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

    // ... (expr_to_datavalue, tests from original file - make sure this is the end of the impl ExecutionEngine block) ...
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::buffer::BufferPoolManager;
    use tempfile::NamedTempFile; // Import moved inside test module
    use tempfile::TempDir; // Import moved inside test module
    
    use crate::catalog::column::Column as CatalogColumn;
    use crate::catalog::table::Table as CatalogTable;
    use crate::transaction::wal::log_manager::{LogManager, LogManagerConfig};
    use crate::transaction::wal::log_buffer::LogBufferConfig;
    use std::path::PathBuf;
    use crate::query::parser::parse;
    use crate::query::parser::ast::Statement;
    
    fn setup_engine_with_tables(tables_to_create: Vec<(&str, Vec<CatalogColumn>)>) -> (ExecutionEngine, NamedTempFile, Arc<LogManager>) {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap().to_string();

        let temp_log_dir = TempDir::new().unwrap();
        let log_config = LogManagerConfig {
            log_dir: PathBuf::from(temp_log_dir.path()),
            log_file_base_name: "test_wal".to_string(),
            max_log_file_size: 1024 * 10,
            buffer_config: LogBufferConfig::default(),
            force_sync: false,
        };
        let log_manager = Arc::new(LogManager::new(log_config).unwrap());
        
        let buffer_pool = Arc::new(BufferPoolManager::new_with_wal(100, &db_path, log_manager.clone()).unwrap()); // Pass db_path by ref
        let catalog_instance = Arc::new(RwLock::new(Catalog::new()));
        {
            let mut catalog_guard = catalog_instance.write().unwrap(); // Use mut for create_table
            for (table_name, cols) in tables_to_create {
                let table = CatalogTable::new(table_name.to_string(), cols);
                catalog_guard.create_table(table).unwrap();
            }
        }
        
        let transaction_manager = Arc::new(TransactionManager::new(log_manager.clone()));
        let engine = ExecutionEngine::new(buffer_pool.clone(), catalog_instance.clone(), transaction_manager);
        (engine, temp_file, log_manager)
    }

    #[test]
    fn test_execute_insert_simple_record_written() {
        let columns = vec![
            CatalogColumn::new("id".to_string(), crate::catalog::schema::DataType::Integer, false, true, None),
            CatalogColumn::new("name".to_string(), crate::catalog::schema::DataType::Text, false, false, None),
        ];
        let (engine, _db_file, _log_manager) = setup_engine_with_tables(vec![("users", columns)]);

        let query = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        let parsed_stmt = parse(query).expect("Failed to parse insert query");
        let result = match parsed_stmt {
            Statement::Insert(insert_stmt) => engine.execute(Statement::Insert(insert_stmt)),
            _ => panic!("Expected InsertStatement from parser"),
        };

        assert!(result.is_ok(), "Insert failed: {:?}", result.err());
        let result_set = result.unwrap();
        assert_eq!(result_set.row_count(), 1);
        let status_row = result_set.rows().first().unwrap();
        let status_text = status_row.get("status").unwrap().to_string();
        
        eprintln!("Insert status: {}", status_text);

        assert!(status_text.contains("INSERT into 'users' successful"), "Status: {}", status_text);
        assert!(status_text.contains("table_id 1"), "Status: {}", status_text); // Table ID starts from 1
        assert!(status_text.contains("page_id 1"), "Status: {}", status_text); // Page ID starts from 1
        assert!(status_text.contains("record_id 0"), "Status: {}", status_text); // Slot num starts from 0

        let catalog_guard = engine.catalog.read().unwrap();
        let table_def = catalog_guard.get_table("users").unwrap();
        assert_eq!(table_def.first_page_id(), Some(1), "Catalog first_page_id not updated correctly");
    }

    #[test]
    fn test_execute_insert_multiple_records_same_page() {
        let columns = vec![
            CatalogColumn::new("val".to_string(), crate::catalog::schema::DataType::Integer, false, false, None),
        ];
        let (engine, _db_file, _log_manager) = setup_engine_with_tables(vec![("data_table", columns)]);

        let query1 = "INSERT INTO data_table (val) VALUES (100)";
        let parsed_stmt1 = parse(query1).expect("Failed to parse insert query1");
        let res1 = match parsed_stmt1 {
            Statement::Insert(insert_stmt) => engine.execute(Statement::Insert(insert_stmt)).unwrap(),
            _ => panic!("Expected InsertStatement from parser for query1"),
        };
        let status1 = res1.rows().first().unwrap().get("status").unwrap().to_string();
        assert!(status1.contains("page_id 1") && status1.contains("record_id 0"));

        let query2 = "INSERT INTO data_table (val) VALUES (200)";
        let parsed_stmt2 = parse(query2).expect("Failed to parse insert query2");
        let res2 = match parsed_stmt2 {
            Statement::Insert(insert_stmt) => engine.execute(Statement::Insert(insert_stmt)).unwrap(),
            _ => panic!("Expected InsertStatement from parser for query2"),
        };
        let status2 = res2.rows().first().unwrap().get("status").unwrap().to_string();
        assert!(status2.contains("page_id 1") && status2.contains("record_id 1"));
        
        let query3 = "INSERT INTO data_table (val) VALUES (300)";
        let parsed_stmt3 = parse(query3).expect("Failed to parse insert query3");
        let res3 = match parsed_stmt3 {
            Statement::Insert(insert_stmt) => engine.execute(Statement::Insert(insert_stmt)).unwrap(),
            _ => panic!("Expected InsertStatement from parser for query3"),
        };
        let status3 = res3.rows().first().unwrap().get("status").unwrap().to_string();
        assert!(status3.contains("page_id 1") && status3.contains("record_id 2"));

        let catalog_guard = engine.catalog.read().unwrap();
        let table_def = catalog_guard.get_table("data_table").unwrap();
        assert_eq!(table_def.first_page_id(), Some(1));
    }

    #[test]
    fn test_execute_insert_page_full() {
        let large_text_col = CatalogColumn::new("data".to_string(), crate::catalog::schema::DataType::Text, true, false, None);
        let (engine, _db_file, _log_manager) = setup_engine_with_tables(vec![("big_data", vec![large_text_col])]);
        
        let large_string_data = "A".repeat(4000); // Adjust size based on PAGE_SIZE if needed
        let sql_string_literal = format!("'{}'", large_string_data);

        let query1 = format!("INSERT INTO big_data (data) VALUES ({})", sql_string_literal);
        let parsed_stmt1 = parse(&query1).expect("Failed to parse insert query1");
        let res1 = match parsed_stmt1 {
            Statement::Insert(insert_stmt) => engine.execute(Statement::Insert(insert_stmt)),
            _ => panic!("Expected InsertStatement from parser for query1"),
        };
        assert!(res1.is_ok(), "Insert 1 failed: {:?}", res1.err());
        let status1 = res1.unwrap().rows().first().unwrap().get("status").unwrap().to_string();
        assert!(status1.contains("page_id 1") && status1.contains("record_id 0"));

        let query2 = format!("INSERT INTO big_data (data) VALUES ({})", sql_string_literal);
        let parsed_stmt2 = parse(&query2).expect("Failed to parse insert query2");
        let res2 = match parsed_stmt2 {
            Statement::Insert(insert_stmt) => engine.execute(Statement::Insert(insert_stmt)),
            _ => panic!("Expected InsertStatement from parser for query2"),
        };
        assert!(res2.is_ok(), "Insert 2 failed: {:?}", res2.err());
        let status2 = res2.unwrap().rows().first().unwrap().get("status").unwrap().to_string();
        assert!(status2.contains("page_id 1") && status2.contains("record_id 1"));

        let query3 = format!("INSERT INTO big_data (data) VALUES ({})", sql_string_literal);
        let parsed_stmt3 = parse(&query3).expect("Failed to parse insert query3");
        let res3 = match parsed_stmt3 {
            Statement::Insert(insert_stmt) => engine.execute(Statement::Insert(insert_stmt)),
            _ => panic!("Expected InsertStatement from parser for query3"),
        };
        assert!(res3.is_err(), "Insert 3 should have failed due to insufficient space, but got: {:?}", res3.ok());
        
        match res3.err().unwrap() {
            QueryError::ExecutionError(msg) => {
                eprintln!("Page full error: {}", msg);
                assert!(msg.contains("Failed to insert record into page") &&
                        msg.contains("Not enough space in page"));
            }
            other_err => panic!("Expected ExecutionError with InsufficientSpace, got {:?}", other_err),
        }
    }

    #[test]
    fn test_alter_table_add_column_execution() {
        let columns = vec![
            CatalogColumn::new("id".to_string(), crate::catalog::schema::DataType::Integer, false, true, None),
        ];
        let (engine, _db_file, _log_manager) = setup_engine_with_tables(vec![("users", columns)]);

        // Add a column
        let alter_sql = "ALTER TABLE users ADD COLUMN name TEXT NOT NULL";
        let parsed_stmt = parse(alter_sql).expect("Failed to parse ALTER TABLE ADD COLUMN");
        let result = match parsed_stmt {
            Statement::Alter(alter_stmt) => engine.execute(Statement::Alter(alter_stmt)),
            _ => panic!("Expected AlterStatement from parser"),
        };
        assert!(result.is_ok(), "ALTER TABLE ADD COLUMN failed: {:?}", result.err());
        let catalog_guard = engine.catalog.read().unwrap();
        let table_def = catalog_guard.get_table("users").unwrap();
        assert!(table_def.has_column("name"), "Column 'name' not found after ALTER TABLE ADD COLUMN");
        let name_col = table_def.get_column("name").unwrap();
        assert_eq!(name_col.data_type(), &crate::catalog::schema::DataType::Text);
        assert!(!name_col.is_nullable(), "Column 'name' should be NOT NULL");
    }

    #[test]
    fn test_alter_table_drop_column_execution() {
        let columns = vec![
            CatalogColumn::new("id".to_string(), crate::catalog::schema::DataType::Integer, false, true, None),
            CatalogColumn::new("name".to_string(), crate::catalog::schema::DataType::Text, false, false, None),
        ];
        let (engine, _db_file, _log_manager) = setup_engine_with_tables(vec![("users", columns)]);

        // Drop a column
        let alter_sql = "ALTER TABLE users DROP COLUMN name";
        let parsed_stmt = parse(alter_sql).expect("Failed to parse ALTER TABLE DROP COLUMN");
        let result = match parsed_stmt {
            Statement::Alter(alter_stmt) => engine.execute(Statement::Alter(alter_stmt)),
            _ => panic!("Expected AlterStatement from parser"),
        };
        assert!(result.is_ok(), "ALTER TABLE DROP COLUMN failed: {:?}", result.err());
        let catalog_guard = engine.catalog.read().unwrap();
        let table_def = catalog_guard.get_table("users").unwrap();
        assert!(!table_def.has_column("name"), "Column 'name' should not exist after DROP COLUMN");
        assert!(table_def.has_column("id"), "Column 'id' should still exist after DROP COLUMN");
    }

    #[test]
    fn test_alter_table_rename_column_execution() {
        let columns = vec![
            CatalogColumn::new("id".to_string(), crate::catalog::schema::DataType::Integer, false, true, None),
            CatalogColumn::new("username".to_string(), crate::catalog::schema::DataType::Text, false, false, None),
        ];
        let (engine, _db_file, _log_manager) = setup_engine_with_tables(vec![("users", columns)]);

        // Rename a column
        let alter_sql = "ALTER TABLE users RENAME COLUMN username TO name";
        let parsed_stmt = parse(alter_sql).expect("Failed to parse ALTER TABLE RENAME COLUMN");
        let result = match parsed_stmt {
            Statement::Alter(alter_stmt) => engine.execute(Statement::Alter(alter_stmt)),
            _ => panic!("Expected AlterStatement from parser"),
        };
        assert!(result.is_ok(), "ALTER TABLE RENAME COLUMN failed: {:?}", result.err());
        let catalog_guard = engine.catalog.read().unwrap();
        let table_def = catalog_guard.get_table("users").unwrap();
        assert!(!table_def.has_column("username"), "Old column name should not exist after rename");
        assert!(table_def.has_column("name"), "New column name should exist after rename");
        let name_col = table_def.get_column("name").unwrap();
        assert_eq!(name_col.data_type(), &crate::catalog::schema::DataType::Text);
    }

    #[test]
    fn test_alter_table_data_migration() {
        use crate::query::parser::ast::Statement;
        use crate::catalog::Column as CatalogColumn;

        // 1. Create table and insert data
        let columns = vec![
            CatalogColumn::new("id".to_string(), crate::catalog::schema::DataType::Integer, false, true, None),
            CatalogColumn::new("name".to_string(), crate::catalog::schema::DataType::Text, false, false, None),
        ];
        let (engine, _db_file, _log_manager) = setup_engine_with_tables(vec![("users", columns)]);

        // Insert a row
        let insert_sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        let parsed_stmt = parse(insert_sql).expect("Failed to parse insert");
        let result = match parsed_stmt {
            Statement::Insert(insert_stmt) => engine.execute(Statement::Insert(insert_stmt)),
            _ => panic!("Expected InsertStatement from parser"),
        };
        assert!(result.is_ok(), "Insert failed: {:?}", result.err());

        // 2. Add a column
        let alter_sql_add = "ALTER TABLE users ADD COLUMN age INTEGER";
        let parsed_stmt_add = parse(alter_sql_add).expect("Failed to parse ALTER TABLE ADD COLUMN");
        let result_add = match parsed_stmt_add {
            Statement::Alter(alter_stmt) => engine.execute(Statement::Alter(alter_stmt)),
            _ => panic!("Expected AlterStatement from parser for ADD"),
        };
        assert!(result_add.is_ok(), "ALTER TABLE ADD COLUMN failed: {:?}", result_add.err());

        // 3. Query and check that 'age' is NULL for existing row
        let select_result_before_drop = engine.execute_query("SELECT * FROM users").expect("SELECT after ADD COLUMN");
        assert_eq!(select_result_before_drop.row_count(), 1);
        let row_before_drop = select_result_before_drop.rows().first().unwrap();
        assert_eq!(row_before_drop.get("id"), Some(&DataValue::Integer(1)));
        assert_eq!(row_before_drop.get("name"), Some(&DataValue::Text("Alice".to_string())));
        assert_eq!(row_before_drop.get("age"), Some(&DataValue::Null));

        // 4. Drop the 'name' column
        let alter_sql_drop = "ALTER TABLE users DROP COLUMN name";
        let parsed_stmt_drop = parse(alter_sql_drop).expect("Failed to parse ALTER TABLE DROP COLUMN");
        let result_drop = match parsed_stmt_drop {
            Statement::Alter(alter_stmt) => engine.execute(Statement::Alter(alter_stmt)),
            _ => panic!("Expected AlterStatement from parser for DROP"),
        };
        assert!(result_drop.is_ok(), "ALTER TABLE DROP COLUMN failed: {:?}", result_drop.err());
        
        let select_result_after_drop = engine.execute_query("SELECT * FROM users").expect("SELECT after DROP COLUMN");
        assert_eq!(select_result_after_drop.row_count(), 1);
        let row_after_drop = select_result_after_drop.rows().first().unwrap();
        assert_eq!(row_after_drop.get("id"), Some(&DataValue::Integer(1)));
        // After data migration for DROP COLUMN, the 'age' column should retain its original value (Null)
        assert_eq!(row_after_drop.get("age"), Some(&DataValue::Null)); 
        assert!(row_after_drop.get("name").is_none());

        // 5. Rename 'age' to 'years'
        // The 'age' column currently holds Null. Renaming it will carry this value.
        let alter_sql_rename = "ALTER TABLE users RENAME COLUMN age TO years";
        let parsed_stmt_rename = parse(alter_sql_rename).expect("Failed to parse ALTER TABLE RENAME COLUMN");
        let result_rename = match parsed_stmt_rename {
            Statement::Alter(alter_stmt) => engine.execute(Statement::Alter(alter_stmt)),
            _ => panic!("Expected AlterStatement from parser for RENAME"),
        };
        assert!(result_rename.is_ok(), "ALTER TABLE RENAME COLUMN failed: {:?}", result_rename.err());
        
        let select_result_after_rename = engine.execute_query("SELECT * FROM users").expect("SELECT after RENAME COLUMN");
        assert_eq!(select_result_after_rename.row_count(), 1);
        let row_after_rename = select_result_after_rename.rows().first().unwrap();
        assert_eq!(row_after_rename.get("id"), Some(&DataValue::Integer(1)));
        assert_eq!(row_after_rename.get("years"), Some(&DataValue::Null)); // Value (Null) persists through rename
        assert!(row_after_rename.get("age").is_none());
    }

    #[test]
    fn test_alter_table_add_column_with_default_data_migration() {
        use crate::query::parser::ast::Statement;
        // 1. Create table and insert data
        let columns = vec![
            CatalogColumn::new("id".to_string(), crate::catalog::schema::DataType::Integer, false, true, None),
            CatalogColumn::new("name".to_string(), crate::catalog::schema::DataType::Text, false, false, None),
        ];
        let (engine, _db_file, _log_manager) = setup_engine_with_tables(vec![("users", columns)]);

        // Insert two rows
        let insert_sql1 = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        let insert_sql2 = "INSERT INTO users (id, name) VALUES (2, 'Bob')";
        let parsed_stmt1 = parse(insert_sql1).expect("Failed to parse insert 1");
        let parsed_stmt2 = parse(insert_sql2).expect("Failed to parse insert 2");
        let result1 = match parsed_stmt1 {
            Statement::Insert(insert_stmt) => engine.execute(Statement::Insert(insert_stmt)),
            _ => panic!("Expected InsertStatement from parser 1"),
        };
        let result2 = match parsed_stmt2 {
            Statement::Insert(insert_stmt) => engine.execute(Statement::Insert(insert_stmt)),
            _ => panic!("Expected InsertStatement from parser 2"),
        };
        assert!(result1.is_ok(), "Insert 1 failed: {:?}", result1.err());
        assert!(result2.is_ok(), "Insert 2 failed: {:?}", result2.err());

        // 2. Add a column with DEFAULT 42
        let alter_sql = "ALTER TABLE users ADD COLUMN age INTEGER DEFAULT 42";
        let parsed_stmt = parse(alter_sql).expect("Failed to parse ALTER TABLE ADD COLUMN with DEFAULT");
        let result = match parsed_stmt {
            Statement::Alter(alter_stmt) => engine.execute(Statement::Alter(alter_stmt)),
            _ => panic!("Expected AlterStatement from parser"),
        };
        assert!(result.is_ok(), "ALTER TABLE ADD COLUMN with DEFAULT failed: {:?}", result.err());

        // 3. Query and check that 'age' is 42 for all existing rows
        let select_result = engine.execute_query("SELECT * FROM users").expect("SELECT after ADD COLUMN with DEFAULT");
        assert_eq!(select_result.row_count(), 2);
        for row in select_result.rows() {
            assert!(row.get("age").is_some(), "Row missing 'age' column");
            assert_eq!(row.get("age"), Some(&DataValue::Integer(42)), "Row 'age' column is not 42");
        }
    }

    #[test]
    fn test_alter_table_data_migration_rename() {
        use crate::query::parser::ast::Statement;
        // 1. Create table and insert data
        let columns = vec![
            CatalogColumn::new("id".to_string(), crate::catalog::schema::DataType::Integer, false, true, None),
            CatalogColumn::new("name".to_string(), crate::catalog::schema::DataType::Text, false, false, None),
        ];
        let (engine, _db_file, _log_manager) = setup_engine_with_tables(vec![("users", columns)]);

        // Insert a row
        let insert_sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        let parsed_stmt = parse(insert_sql).expect("Failed to parse insert");
        let result = match parsed_stmt {
            Statement::Insert(insert_stmt) => engine.execute(Statement::Insert(insert_stmt)),
            _ => panic!("Expected InsertStatement from parser"),
        };
        assert!(result.is_ok(), "Insert failed: {:?}", result.err());

        // 2. Add a column
        let alter_sql_add = "ALTER TABLE users ADD COLUMN age INTEGER"; // Note: changed from original 'age' to 'age_new' then back
        let parsed_stmt_add = parse(alter_sql_add).expect("Failed to parse ALTER TABLE ADD COLUMN");
        let result_add = match parsed_stmt_add {
            Statement::Alter(alter_stmt) => engine.execute(Statement::Alter(alter_stmt)),
            _ => panic!("Expected AlterStatement from parser for ADD"),
        };
        assert!(result_add.is_ok(), "ALTER TABLE ADD COLUMN failed: {:?}", result_add.err());

        // 3. Query and check that 'age' is NULL for existing row
        let select_result_before_drop = engine.execute_query("SELECT * FROM users").expect("SELECT after ADD COLUMN");
        assert_eq!(select_result_before_drop.row_count(), 1);
        let row_before_drop = select_result_before_drop.rows().first().unwrap();
        assert_eq!(row_before_drop.get("id"), Some(&DataValue::Integer(1)));
        assert_eq!(row_before_drop.get("name"), Some(&DataValue::Text("Alice".to_string())));
        assert_eq!(row_before_drop.get("age"), Some(&DataValue::Null));


        // 4. Drop the 'name' column
        let alter_sql_drop = "ALTER TABLE users DROP COLUMN name";
        let parsed_stmt_drop = parse(alter_sql_drop).expect("Failed to parse ALTER TABLE DROP COLUMN");
        let result_drop = match parsed_stmt_drop {
            Statement::Alter(alter_stmt) => engine.execute(Statement::Alter(alter_stmt)),
            _ => panic!("Expected AlterStatement from parser for DROP"),
        };
        assert!(result_drop.is_ok(), "ALTER TABLE DROP COLUMN failed: {:?}", result_drop.err());
        
        let select_result_after_drop = engine.execute_query("SELECT * FROM users").expect("SELECT after DROP COLUMN");
        assert_eq!(select_result_after_drop.row_count(), 1);
        let row_after_drop = select_result_after_drop.rows().first().unwrap();
        assert_eq!(row_after_drop.get("id"), Some(&DataValue::Integer(1)));
        // After data migration for DROP COLUMN, the 'age' column should retain its original value (Null)
        assert_eq!(row_after_drop.get("age"), Some(&DataValue::Null)); 
        assert!(row_after_drop.get("name").is_none());

        // 5. Rename 'age' to 'years'
        // The 'age' column currently holds Null. Renaming it will carry this value.
        let alter_sql_rename = "ALTER TABLE users RENAME COLUMN age TO years";
        let parsed_stmt_rename = parse(alter_sql_rename).expect("Failed to parse ALTER TABLE RENAME COLUMN");
        let result_rename = match parsed_stmt_rename {
            Statement::Alter(alter_stmt) => engine.execute(Statement::Alter(alter_stmt)),
            _ => panic!("Expected AlterStatement from parser for RENAME"),
        };
        assert!(result_rename.is_ok(), "ALTER TABLE RENAME COLUMN failed: {:?}", result_rename.err());
        
        let select_result_after_rename = engine.execute_query("SELECT * FROM users").expect("SELECT after RENAME COLUMN");
        assert_eq!(select_result_after_rename.row_count(), 1);
        let row_after_rename = select_result_after_rename.rows().first().unwrap();
        assert_eq!(row_after_rename.get("id"), Some(&DataValue::Integer(1)));
        assert_eq!(row_after_rename.get("years"), Some(&DataValue::Null)); // Value (Null) persists through rename
        assert!(row_after_rename.get("age").is_none());
    }
} 