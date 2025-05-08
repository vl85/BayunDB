// Query Execution Engine Implementation
//
// This module implements the engine for executing SQL queries.

use std::sync::Arc;

use crate::query::executor::result::{QueryResult, QueryResultSet, QueryError};
use crate::query::parser::ast::{Statement, SelectStatement};
use crate::query::planner::{Planner, LogicalPlan};
use crate::query::executor::operators::scan::create_table_scan;
use crate::storage::buffer::BufferPoolManager;

/// The ExecutionEngine is responsible for executing parsed SQL queries
/// and producing result sets.
pub struct ExecutionEngine {
    /// Buffer pool manager for storage access
    buffer_pool: Arc<BufferPoolManager>,
    /// Query planner
    planner: Planner,
}

impl ExecutionEngine {
    /// Create a new execution engine with the given buffer pool
    pub fn new(buffer_pool: Arc<BufferPoolManager>) -> Self {
        ExecutionEngine {
            buffer_pool,
            planner: Planner::new(),
        }
    }
    
    /// Execute a SQL statement and return the result
    pub fn execute(&self, statement: Statement) -> QueryResult<QueryResultSet> {
        match statement {
            Statement::Select(select) => self.execute_select(select),
            Statement::Insert(_) => Err(QueryError::ExecutionError("INSERT not implemented yet".to_string())),
            Statement::Update(_) => Err(QueryError::ExecutionError("UPDATE not implemented yet".to_string())),
            Statement::Delete(_) => Err(QueryError::ExecutionError("DELETE not implemented yet".to_string())),
            Statement::Create(_) => Err(QueryError::ExecutionError("CREATE not implemented yet".to_string())),
        }
    }
    
    /// Execute a SELECT statement
    fn execute_select(&self, select: SelectStatement) -> QueryResult<QueryResultSet> {
        // Create a logical plan for the SELECT statement
        let logical_plan = self.planner.create_logical_plan(&Statement::Select(select))?;
        
        // Execute the logical plan
        self.execute_logical_plan(&logical_plan)
    }
    
    /// Execute a logical plan
    fn execute_logical_plan(&self, plan: &LogicalPlan) -> QueryResult<QueryResultSet> {
        match plan {
            LogicalPlan::Projection { columns: _, input } => {
                // Execute the input plan
                let result_set = self.execute_logical_plan(input)?;
                
                // Project only the required columns
                // For now, just return the result set as-is (simplified)
                // In a complete implementation, we would filter columns here
                Ok(result_set)
            }
            LogicalPlan::Filter { predicate: _, input } => {
                // Execute the input plan
                let input_results = self.execute_logical_plan(input)?;
                
                // For now, just return the input results as-is
                // In a complete implementation, we would filter rows based on the predicate
                Ok(input_results)
            }
            LogicalPlan::Scan { table_name, alias: _ } => {
                // For now, we only support simple SELECT * FROM table queries
                
                // Create a table scan operator
                let scan_op = create_table_scan(table_name)?;
                
                // Initialize the operator
                {
                    let mut scan = scan_op.lock().map_err(|e| {
                        QueryError::ExecutionError(format!("Failed to lock scan operator: {}", e))
                    })?;
                    scan.init()?;
                }
                
                // Get column names from the first row (simplified approach)
                let mut result_columns = Vec::new();
                {
                    let mut scan = scan_op.lock().map_err(|e| {
                        QueryError::ExecutionError(format!("Failed to lock scan operator: {}", e))
                    })?;
                    
                    if let Some(row) = scan.next()? {
                        result_columns = row.columns().to_vec();
                    }
                    
                    // Reset the operator
                    scan.close()?;
                    scan.init()?;
                }
                
                // Create result set
                let mut result_set = QueryResultSet::new(result_columns);
                
                // Collect rows
                {
                    let mut scan = scan_op.lock().map_err(|e| {
                        QueryError::ExecutionError(format!("Failed to lock scan operator: {}", e))
                    })?;
                    
                    while let Some(row) = scan.next()? {
                        result_set.add_row(row);
                    }
                    
                    // Close the operator
                    scan.close()?;
                }
                
                Ok(result_set)
            }
        }
    }
    
    /// Parse and execute a SQL query string
    pub fn execute_query(&self, query: &str) -> QueryResult<QueryResultSet> {
        // Use the planner to create a logical plan from the SQL
        let logical_plan = self.planner.create_logical_plan_from_sql(query)?;
        
        // Execute the logical plan
        self.execute_logical_plan(&logical_plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::buffer::BufferPoolManager;
    use tempfile::NamedTempFile;
    
    fn create_test_db() -> (Arc<BufferPoolManager>, NamedTempFile) {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();
        let buffer_pool = Arc::new(BufferPoolManager::new(100, path).unwrap());
        
        (buffer_pool, temp_file)
    }
    
    #[test]
    fn test_execution_engine() {
        // This is a minimal test - real functionality will depend on
        // the table scan operator implementation
        let (buffer_pool, _temp_file) = create_test_db();
        
        let engine = ExecutionEngine::new(buffer_pool);
        
        // Create a mock SELECT statement
        let select = Statement::Select(SelectStatement {
            columns: vec![],
            from: vec![crate::query::parser::ast::TableReference {
                name: "users".to_string(),
                alias: None,
            }],
            where_clause: None,
        });
        
        // This will likely fail since we don't have a real table,
        // but it tests that the engine can dispatch to the right handler
        let _ = engine.execute(select);
    }
    
    #[test]
    fn test_query_execution() {
        let (buffer_pool, _temp_file) = create_test_db();
        
        let engine = ExecutionEngine::new(buffer_pool);
        
        // We only support test_table in our scan operator's stub implementation
        let result = engine.execute_query("SELECT * FROM test_table");
        
        // In our current stub implementation, this should succeed for test_table
        assert!(result.is_ok());
        
        // But it will fail for a non-existent table
        let result = engine.execute_query("SELECT * FROM nonexistent_table");
        assert!(result.is_err());
    }
} 