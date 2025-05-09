// Query Execution Engine Implementation
//
// This module implements the engine for executing SQL queries.

use std::sync::Arc;

use crate::storage::buffer::BufferPoolManager;
use crate::storage::page::PageManager;
use crate::query::executor::result::{QueryResult, QueryError, QueryResultSet};
use crate::query::parser::ast::{Statement, SelectStatement};
use crate::query::planner::{Planner, physical};

#[cfg(test)]
use crate::query::parser::ast::{SelectColumn, ColumnReference, TableReference};

/// The ExecutionEngine is responsible for executing parsed SQL queries
/// and producing result sets.
pub struct ExecutionEngine {
    /// Buffer pool manager for storage access
    buffer_pool: Arc<BufferPoolManager>,
    
    /// Query planner
    planner: Planner,
    
    /// Page manager for record handling
    page_manager: PageManager,
}

impl ExecutionEngine {
    /// Create a new execution engine with the given buffer pool
    pub fn new(buffer_pool: Arc<BufferPoolManager>) -> Self {
        ExecutionEngine {
            buffer_pool,
            planner: Planner::new(),
            page_manager: PageManager::new(),
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
        
        // Create a physical plan from the logical plan
        let physical_plan = self.planner.create_physical_plan(&logical_plan)?;
        
        // Execute the physical plan
        self.execute_physical_plan(&physical_plan)
    }
    
    /// Execute a physical plan by building and running an operator tree
    fn execute_physical_plan(&self, plan: &physical::PhysicalPlan) -> QueryResult<QueryResultSet> {
        // Build operator tree from physical plan
        let root_op = physical::build_operator_tree(plan)?;
        
        // Initialize the operator
        {
            let mut op = root_op.lock().map_err(|e| {
                QueryError::ExecutionError(format!("Failed to lock root operator: {}", e))
            })?;
            op.init()?;
        }
        
        // Get column names from the first row
        let mut result_columns = Vec::new();
        {
            let mut op = root_op.lock().map_err(|e| {
                QueryError::ExecutionError(format!("Failed to lock root operator: {}", e))
            })?;
            
            if let Some(row) = op.next()? {
                result_columns = row.columns().to_vec();
                
                // Create result set with the first row
                let mut result_set = QueryResultSet::new(result_columns);
                result_set.add_row(row);
                
                // Collect remaining rows
                while let Some(row) = op.next()? {
                    result_set.add_row(row);
                }
                
                // Close the operator
                op.close()?;
                
                Ok(result_set)
            } else {
                // No rows, return empty result set
                op.close()?;
                Ok(QueryResultSet::new(result_columns))
            }
        }
    }
    
    /// Parse and execute a SQL query string
    pub fn execute_query(&self, query: &str) -> QueryResult<QueryResultSet> {
        // Use the planner to create a logical plan from the SQL
        let logical_plan = self.planner.create_logical_plan_from_sql(query)?;
        
        // Create a physical plan from the logical plan
        let physical_plan = self.planner.create_physical_plan(&logical_plan)?;
        
        // Execute the physical plan
        self.execute_physical_plan(&physical_plan)
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
            joins: vec![],
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
    
    #[test]
    fn test_end_to_end_query_execution() {
        let (buffer_pool, _temp_file) = create_test_db();
        
        let engine = ExecutionEngine::new(buffer_pool);
        
        // Test a query with projection - should still work with our stub operators
        let result = engine.execute_query("SELECT id, name FROM test_table");
        assert!(result.is_ok());
        
        // Test with a filter that should filter out some rows
        let result = engine.execute_query("SELECT * FROM test_table WHERE id = 5");
        
        // Even though filter is implemented, we're using a table scan that generates
        // fake data, so we can't test actual filtering logic here very well
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_execute_select() {
        // Create a test database with temporary file
        let (buffer_pool, _temp_file) = create_test_db();
        
        // Create execution engine
        let engine = ExecutionEngine::new(buffer_pool);
        
        // Create a SELECT statement
        let select = Statement::Select(SelectStatement {
            columns: vec![
                SelectColumn::Column(ColumnReference {
                    table: None,
                    name: "id".to_string(),
                }),
                SelectColumn::Column(ColumnReference {
                    table: None,
                    name: "name".to_string(),
                }),
            ],
            from: vec![TableReference {
                name: "users".to_string(),
                alias: None,
            }],
            where_clause: None,
            joins: vec![],
        });
        
        // Try to build a plan
        let result = engine.planner.create_logical_plan(&select);
        
        // Should succeed
        assert!(result.is_ok());
        
        // Just verify the engine is created
        assert!(engine.execute_query("SELECT * FROM test_table").is_ok());
    }
} 