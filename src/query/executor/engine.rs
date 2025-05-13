// Query Execution Engine Implementation
//
// This module implements the engine for executing SQL queries.

use std::sync::Arc;
use std::sync::RwLock;

use crate::storage::buffer::BufferPoolManager;
use crate::storage::page::PageManager;
use crate::query::executor::result::{QueryResult, QueryError, QueryResultSet, DataValue, Row};
use crate::query::parser::ast::{Statement, SelectStatement, Expression};
use crate::query::parser::parse;
use crate::catalog::Catalog;
use crate::transaction::concurrency::transaction_manager::TransactionManager;
use crate::query::executor::operators::Operator;
use crate::query::planner::physical_plan::PhysicalPlan;
use crate::query::planner::Planner;
use crate::query::planner::operator_builder::OperatorBuilder;
use crate::query::executor::dml_executor::DmlExecutor;
use crate::query::executor::ddl_executor::DdlExecutor;
use crate::transaction::wal::log_manager::{LogManager, LogManagerConfig};

pub struct ExecutionEngine {
    buffer_pool: Arc<BufferPoolManager>,
    catalog: Arc<RwLock<Catalog>>,
    transaction_manager: Arc<TransactionManager>,
    planner: Planner,
    page_manager: PageManager,
    operator_builder: OperatorBuilder,
    dml_executor: DmlExecutor,
    ddl_executor: DdlExecutor,
}

impl ExecutionEngine {
    pub fn new(
        buffer_pool: Arc<BufferPoolManager>, 
        catalog: Arc<RwLock<Catalog>>, 
        transaction_manager: Arc<TransactionManager>
    ) -> Self {
        let planner = Planner::new(buffer_pool.clone(), catalog.clone());
        let page_manager = PageManager::new();
        let operator_builder = OperatorBuilder::new(buffer_pool.clone(), catalog.clone());
        let dml_executor = DmlExecutor::new(buffer_pool.clone(), catalog.clone(), transaction_manager.clone());
        let ddl_executor = DdlExecutor::new(buffer_pool.clone(), catalog.clone());
        
        ExecutionEngine {
            buffer_pool,
            catalog,
            transaction_manager,
            planner,
            page_manager,
            operator_builder,
            dml_executor,
            ddl_executor,
        }
    }
    
    fn plan_is_aggregate(plan: &PhysicalPlan) -> bool {
        matches!(plan, PhysicalPlan::HashAggregate { .. }) 
    }
    
    pub fn execute(&self, statement: Statement) -> QueryResult<QueryResultSet> {
        match statement {
            Statement::Select(select) => self.execute_select(select),
            Statement::Insert(insert) => self.dml_executor.execute_insert(insert),
            Statement::Update(update) => self.dml_executor.execute_update(update),
            Statement::Delete(delete) => self.dml_executor.execute_delete(delete),
            Statement::Create(create) => self.ddl_executor.execute_create(create),
            Statement::Alter(alter_stmt) => {
                if !self.transaction_manager.get_active_transaction_ids().is_empty() {
                    return Err(QueryError::ExecutionError("DDL operations are blocked until all transactions complete (no active transactions allowed during schema changes)".to_string()));
                }
                self.ddl_executor.execute_alter(alter_stmt)
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

    pub fn execute_query(&self, query: &str) -> QueryResult<QueryResultSet> {
        let statement = parse(query)?;
        match &statement {
            Statement::Insert(insert_stmt) => {
                // For INSERT, we directly call execute_insert for now
                // This bypasses the planner for INSERT as it's simpler
                // TODO: Eventually, INSERT might also go through a planner for features like INSERT FROM SELECT
                return self.dml_executor.execute_insert(insert_stmt.clone());
            }
            Statement::Create(create_stmt) => {
                // Similar to INSERT, CREATE is directly executed.
                return self.ddl_executor.execute_create(create_stmt.clone());
            }
            Statement::Update(update_stmt) => {
                // Bypass planner for UPDATE for now
                return self.dml_executor.execute_update(update_stmt.clone());
            }
            Statement::Delete(delete_stmt) => {
                // Bypass planner for DELETE for now
                return self.dml_executor.execute_delete(delete_stmt.clone());
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, RwLock};
    use tempfile::{NamedTempFile, TempDir};
    use crate::catalog::Column;
    use crate::transaction::wal::log_manager::{LogManager, LogManagerConfig};
    use std::path::PathBuf;
    
    fn setup_engine_with_tables(tables_to_create: Vec<(&str, Vec<Column>)>) -> (ExecutionEngine, NamedTempFile, Arc<LogManager>) {
        // Create temporary file for log manager
        let log_file = NamedTempFile::new().unwrap();
        let log_file_path = log_file.path().to_str().unwrap().to_string();
        
        // Create temporary directory for database
        let temp_db_dir = TempDir::new().unwrap();
        let db_path = temp_db_dir.path().join("test.db");
        
        // Initialize components
        let buffer_pool = Arc::new(BufferPoolManager::new(10, db_path).unwrap());
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        
        // Create log manager with default config
        let log_config = LogManagerConfig::default();
        let log_manager = Arc::new(LogManager::new(log_config).unwrap());
        let transaction_manager = Arc::new(TransactionManager::new(log_manager.clone()));
        
        // Create tables
        {
            let mut catalog_guard = catalog.write().unwrap();
            for (table_name, columns) in tables_to_create {
                let table = crate::catalog::Table::new(table_name.to_string(), columns);
                catalog_guard.create_table(table).unwrap();
            }
        }
        
        let engine = ExecutionEngine::new(buffer_pool, catalog, transaction_manager);
        
        (engine, log_file, log_manager)
    }
} 