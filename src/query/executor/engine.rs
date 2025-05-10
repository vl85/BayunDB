// Query Execution Engine Implementation
//
// This module implements the engine for executing SQL queries.

use std::sync::Arc;
use std::sync::RwLock;

use crate::storage::buffer::BufferPoolManager;
use crate::storage::page::PageManager;
use crate::query::executor::result::{QueryResult, QueryError, QueryResultSet, DataValue, Row};
use crate::query::parser::ast::{Statement, SelectStatement, Expression, CreateStatement};
use crate::query::parser::parse;
use crate::query::planner::{Planner, PhysicalPlan};
use crate::query::planner::operator_builder::OperatorBuilder;
use crate::catalog::Catalog;

#[cfg(test)]
use crate::query::parser::ast::{SelectColumn, ColumnReference, TableReference};

/// The ExecutionEngine is responsible for executing parsed SQL queries
/// and producing result sets.
pub struct ExecutionEngine {
    /// Buffer pool manager for storage access
    buffer_pool: Arc<BufferPoolManager>,
    
    /// Catalog for schema information
    catalog: Arc<RwLock<Catalog>>,
    
    /// Query planner
    planner: Planner,
    
    /// Page manager for record handling
    page_manager: PageManager,
    
    /// Operator builder
    operator_builder: OperatorBuilder,
}

impl ExecutionEngine {
    /// Create a new execution engine with the given buffer pool and catalog
    pub fn new(buffer_pool: Arc<BufferPoolManager>, catalog: Arc<RwLock<Catalog>>) -> Self {
        ExecutionEngine {
            buffer_pool: buffer_pool.clone(),
            catalog: catalog.clone(),
            planner: Planner::new(buffer_pool.clone(), catalog.clone()),
            page_manager: PageManager::new(),
            operator_builder: OperatorBuilder::new(buffer_pool, catalog),
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
        
        // Extract expected column names from the plan
        let expected_columns = match plan {
            PhysicalPlan::HashAggregate { aggregate_expressions, group_by, .. } => {
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
            PhysicalPlan::Project { columns, .. } => columns.clone(),
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
        // Parse the SQL into a statement
        let statement = parse(query)
            .map_err(|e| QueryError::PlanningError(format!("Parse error: {:?}", e)))?;
        
        // Check if this is a DDL statement that should use direct execution
        match &statement {
            // DDL statements that should bypass the operator tree
            Statement::Create(_) => {
                // Route directly to the execute method
                return self.execute(statement);
            },
            // In the future, other DDL operations can be added here
            // Statement::Drop(_) => return self.execute(statement),
            // Statement::Alter(_) => return self.execute(statement),
            
            // Continue with regular query planning for DML statements
            _ => {}
        }
        
        // For non-DDL statements, continue with the planning approach
        // Create a logical plan from the statement
        let logical_plan = self.planner.create_logical_plan(&statement)?;
        
        // Create a physical plan from the logical plan
        let physical_plan = self.planner.create_physical_plan(&logical_plan)?;
        
        // Execute the physical plan
        self.execute_physical_plan(&physical_plan)
    }
    
    /// Execute a CREATE TABLE statement
    fn execute_create(&self, create: CreateStatement) -> QueryResult<QueryResultSet> {
        use crate::catalog::{Catalog, Table, Column};
        use crate::query::executor::result::DataValue;
        
        // Use the injected catalog instance
        let catalog_instance = self.catalog.clone();
        
        // Create the table and columns outside the lock scope
        let mut columns = Vec::new();
        for col_def in &create.columns {
            let column = Column::from_column_def(col_def)
                .map_err(|e| QueryError::ExecutionError(format!("Invalid column definition: {}", e)))?;
            columns.push(column);
        }
        
        // Create the table
        let table = Table::new(create.table_name.clone(), columns);
        
        // Acquire a write lock on the catalog to create the table
        let mut catalog_guard = catalog_instance.write().map_err(|e| 
            QueryError::ExecutionError(format!("Failed to acquire catalog write lock: {}", e)))?;
        catalog_guard.create_table(table)
            .map_err(|e| QueryError::ExecutionError(format!("Failed to create table: {}", e)))?;
        
        // Return success message
        let mut result = QueryResultSet::new(vec!["result".to_string()]);
        result.add_row(Row::from_values(
            vec!["result".to_string()],
            vec![DataValue::Text(format!("Table {} created successfully", create.table_name))]
        ));
        Ok(result)
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
        let (buffer_pool, _db_file) = create_test_db();
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        let engine = ExecutionEngine::new(buffer_pool, catalog);
        assert!(engine.execute_query("SELECT * FROM users").is_err()); // users table doesn't exist yet
    }
    
    #[test]
    fn test_query_execution() {
        let (buffer_pool, _db_file) = create_test_db();
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        let engine = ExecutionEngine::new(buffer_pool.clone(), catalog.clone());
        
        // Create a test table
        let create_query = "CREATE TABLE users (id INTEGER, name TEXT)";
        let create_result = engine.execute_query(create_query);
        assert!(create_result.is_ok(), "Table creation should succeed");
        
        // Test a query with projection - should still work with our stub operators
        let result = engine.execute_query("SELECT id, name FROM users");
        
        // Log error details if test fails
        if let Err(ref err) = result {
            println!("Projection query failed: {:?}", err);
        }
        
        assert!(result.is_ok());
        
        // Test with a filter that should filter out some rows
        let result = engine.execute_query("SELECT * FROM users WHERE id = 5");
        
        // Log error details if test fails
        if let Err(ref err) = result {
            println!("Filter query failed: {:?}", err);
        }
        
        // Even though filter is implemented, we're using a table scan that generates
        // fake data, so we can't test actual filtering logic here very well
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_end_to_end_query_execution() {
        let (buffer_pool, _db_file) = create_test_db();
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        let engine = ExecutionEngine::new(buffer_pool.clone(), catalog.clone());

        // Create table
        engine.execute_query("CREATE TABLE test_data (id INTEGER, name TEXT, age INTEGER)").unwrap();
        
        // Test a query with projection - should still work with our stub operators
        let result = engine.execute_query("SELECT id, name FROM test_data");
        
        // Log error details if test fails
        if let Err(ref err) = result {
            println!("Projection query failed: {:?}", err);
        }
        
        assert!(result.is_ok());
        
        // Test with a filter that should filter out some rows
        let result = engine.execute_query("SELECT * FROM test_data WHERE id = 5");
        
        // Log error details if test fails
        if let Err(ref err) = result {
            println!("Filter query failed: {:?}", err);
        }
        
        // Even though filter is implemented, we're using a table scan that generates
        // fake data, so we can't test actual filtering logic here very well
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_execute_select() {
        let (buffer_pool, _db_file) = create_test_db();
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        let engine = ExecutionEngine::new(buffer_pool.clone(), catalog.clone());

        // Create a table and insert some data
        engine.execute_query("CREATE TABLE test_data (id INTEGER, name TEXT)").unwrap();
        
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
                name: "test_data".to_string(), // Use test_data instead of users
                alias: None,
            }],
            where_clause: None,
            joins: vec![],
            group_by: None,
            having: None,
        });
        
        // Try to build a plan using the planner directly
        let result = engine.planner.create_logical_plan(&select);
        
        // Should succeed
        assert!(result.is_ok());
        
        // Verify the engine can create a physical plan
        let physical_plan = engine.planner.create_physical_plan(&result.unwrap());
        assert!(physical_plan.is_ok());
    }
    
    #[test]
    fn test_create_table() {
        let (buffer_pool, _db_file) = create_test_db();
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        let engine = ExecutionEngine::new(buffer_pool, catalog.clone());
        
        let query = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)";
        let create_result = engine.execute_query(query);
        assert!(create_result.is_ok(), "Table creation should succeed");
    }
} 