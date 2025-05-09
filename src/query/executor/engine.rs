// Query Execution Engine Implementation
//
// This module implements the engine for executing SQL queries.

use std::sync::Arc;

use crate::storage::buffer::BufferPoolManager;
use crate::storage::page::PageManager;
use crate::query::executor::result::{QueryResult, QueryError, QueryResultSet, DataValue, Row};
use crate::query::parser::ast::{Statement, SelectStatement, Expression, CreateStatement};
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
            Statement::Create(create) => self.execute_create(create),
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
        
        // Extract expected column names from the plan
        let expected_columns = match plan {
            physical::PhysicalPlan::HashAggregate { aggregate_expressions, group_by, .. } => {
                let mut columns = Vec::new();
                
                // Add group by columns first
                for expr in group_by {
                    match expr {
                        Expression::Column(col_ref) => {
                            columns.push(col_ref.name.clone());
                        }
                        Expression::BinaryOp { left, op, right } if *op == crate::query::parser::ast::Operator::Modulo => {
                            if let (Expression::Column(col_ref), Expression::Literal(crate::query::parser::ast::Value::Integer(val))) 
                                = (&**left, &**right) {
                                columns.push(format!("{}_mod_{}", col_ref.name, val));
                            } else {
                                columns.push("expr".to_string());
                            }
                        }
                        _ => {
                            // For other expressions, use a generic name
                            columns.push("expr".to_string());
                        }
                    }
                }
                
                // Then add aggregate columns
                for expr in aggregate_expressions {
                    match expr {
                        Expression::Aggregate { function, arg } => {
                            match function {
                                crate::query::parser::ast::AggregateFunction::Count => {
                                    if arg.is_none() {
                                        // Ensure consistent column name for COUNT(*)
                                        columns.push("COUNT(*)".to_string());
                                    } else if let Some(arg_expr) = arg {
                                        if let Expression::Column(col_ref) = &**arg_expr {
                                            columns.push(format!("COUNT({})", col_ref.name));
                                        } else {
                                            columns.push("COUNT(expr)".to_string()); 
                                        }
                                    }
                                }
                                crate::query::parser::ast::AggregateFunction::Sum => {
                                    if let Some(arg_expr) = arg {
                                        if let Expression::Column(col_ref) = &**arg_expr {
                                            columns.push(format!("SUM({})", col_ref.name));
                                        } else {
                                            columns.push("SUM(expr)".to_string());
                                        }
                                    }
                                }
                                crate::query::parser::ast::AggregateFunction::Avg => {
                                    if let Some(arg_expr) = arg {
                                        if let Expression::Column(col_ref) = &**arg_expr {
                                            columns.push(format!("AVG({})", col_ref.name));
                                        } else {
                                            columns.push("AVG(expr)".to_string());
                                        }
                                    }
                                }
                                crate::query::parser::ast::AggregateFunction::Min => {
                                    if let Some(arg_expr) = arg {
                                        if let Expression::Column(col_ref) = &**arg_expr {
                                            columns.push(format!("MIN({})", col_ref.name));
                                        } else {
                                            columns.push("MIN(expr)".to_string());
                                        }
                                    }
                                }
                                crate::query::parser::ast::AggregateFunction::Max => {
                                    if let Some(arg_expr) = arg {
                                        if let Expression::Column(col_ref) = &**arg_expr {
                                            columns.push(format!("MAX({})", col_ref.name));
                                        } else {
                                            columns.push("MAX(expr)".to_string());
                                        }
                                    }
                                }
                            }
                        }
                        _ => columns.push("expr".to_string()),
                    }
                }
                
                columns
            }
            physical::PhysicalPlan::Project { columns, .. } => columns.clone(),
            _ => Vec::new(),
        };
        
        // Create result set with expected columns
        let mut result_set = QueryResultSet::new(expected_columns.clone());
        
        // Get rows from the operator
        let mut op = root_op.lock().map_err(|e| {
            QueryError::ExecutionError(format!("Failed to lock root operator: {}", e))
        })?;
        
        // Collect all rows
        while let Some(mut row) = op.next()? {
            // Ensure column names consistency for aggregates
            // This ensures the row has the same column names as expected_columns
            for (i, column) in expected_columns.iter().enumerate() {
                if !row.get(column).is_some() {
                    // For COUNT(*), check if there's a COUNT(*) column with a different format
                    if column == "COUNT(*)" {
                        // Get all values to avoid borrowing issues
                        let row_values = row.values();
                        if i < row_values.len() {
                            let count_value = row_values[i].clone();
                            row.set(column.clone(), count_value);
                        }
                    }
                    // For other aggregate columns, try to find a matching column
                    else if column.starts_with("COUNT(") || 
                            column.starts_with("SUM(") || 
                            column.starts_with("AVG(") || 
                            column.starts_with("MIN(") || 
                            column.starts_with("MAX(") {
                        // Collect all column names and values first to avoid borrowing conflicts
                        let column_values: Vec<(String, DataValue)> = row.values_with_names()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        
                        for (col_name, value) in column_values {
                            // Check if this is the right aggregate column by comparing function names
                            if col_name.starts_with(&column[..4]) {
                                row.set(column.clone(), value);
                                break;
                            }
                        }
                    }
                }
            }
            
            result_set.add_row(row);
        }
        
        // Close the operator
        op.close()?;
        
        Ok(result_set)
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
    
    /// Execute a CREATE TABLE statement
    fn execute_create(&self, create: CreateStatement) -> QueryResult<QueryResultSet> {
        use crate::catalog::{Catalog, Table, Column};
        
        // Get the global catalog instance
        let catalog_instance = Catalog::instance();
        let catalog = catalog_instance.write().unwrap();
        
        // Convert AST column definitions to catalog columns
        let mut columns = Vec::new();
        for col_def in &create.columns {
            let column = Column::from_column_def(col_def)
                .map_err(|e| QueryError::ExecutionError(format!("Invalid column definition: {}", e)))?;
            columns.push(column);
        }
        
        // Create the table
        let table = Table::new(create.table_name.clone(), columns);
        
        // Add the table to the catalog
        catalog.create_table(table.clone())
            .map_err(|e| QueryError::ExecutionError(format!("Failed to create table: {}", e)))?;
        
        // Create the table on disk
        // (In a real implementation, this would allocate pages for the table data)
        
        // Return an empty result set
        let mut result_set = QueryResultSet::new(vec!["result".to_string()]);
        result_set.add_row(Row::from_values(
            vec!["result".to_string()],
            vec![DataValue::Text(format!("Table {} created successfully", create.table_name))]
        ));
        
        Ok(result_set)
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
            group_by: None,
            having: None,
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
                    name: "id".to_string()
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
            group_by: None,
            having: None,
        });
        
        // Try to build a plan
        let result = engine.planner.create_logical_plan(&select);
        
        // Should succeed
        assert!(result.is_ok());
        
        // Just verify the engine is created
        assert!(engine.execute_query("SELECT * FROM test_table").is_ok());
    }
} 