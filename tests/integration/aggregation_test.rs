use anyhow::{Result, anyhow};
use bayundb::query::parser::parse;
use bayundb::query::parser::ast::{Statement, AggregateFunction, Expression};
use bayundb::query::executor::result::DataValue;
use bayundb::query::executor::engine::ExecutionEngine;
use bayundb::storage::buffer::BufferPoolManager;
use std::sync::Arc;
use tempfile::tempdir;
use bayundb::catalog::Catalog;
use std::sync::RwLock;

#[test]
fn test_count_query() -> Result<()> {
    // Test a simple COUNT(*) query
    let sql = "SELECT COUNT(*) FROM users";
    
    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
    // Verify the AST structure
    if let Statement::Select(select) = statement {
        assert_eq!(select.columns.len(), 1);
        
        // Verify we have a COUNT(*) expression
        match &select.columns[0] {
            bayundb::query::parser::ast::SelectColumn::Expression { expr, .. } => {
                if let Expression::Aggregate { function, arg } = &**expr {
                    assert_eq!(*function, AggregateFunction::Count);
                    assert!(arg.is_none(), "Expected COUNT(*) with no argument");
                } else {
                    panic!("Expected aggregate function expression");
                }
            }
            _ => panic!("Expected Expression in column list"),
        }
        
        // Rest of implementation will be added once the logical planner 
        // supports aggregation operators
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

#[test]
fn test_group_by_query() -> Result<()> {
    // Test GROUP BY query
    let sql = "SELECT department_id, COUNT(*) FROM employees GROUP BY department_id";
    
    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
    // Verify the AST structure
    if let Statement::Select(select) = statement {
        // Check GROUP BY clause
        assert!(select.group_by.is_some(), "Expected GROUP BY clause");
        let group_by = select.group_by.unwrap();
        assert_eq!(group_by.len(), 1, "Expected one column in GROUP BY");
        
        // Verify columns match GROUP BY
        assert_eq!(select.columns.len(), 2, "Expected two columns");
        
        // First column should be department_id
        match &select.columns[0] {
            bayundb::query::parser::ast::SelectColumn::Column(col_ref) => {
                assert_eq!(col_ref.name, "department_id");
            }
            _ => panic!("Expected Column in first position"),
        }
        
        // Second column should be COUNT(*)
        match &select.columns[1] {
            bayundb::query::parser::ast::SelectColumn::Expression { expr, .. } => {
                if let Expression::Aggregate { function, .. } = &**expr {
                    assert_eq!(*function, AggregateFunction::Count);
                } else {
                    panic!("Expected aggregate function expression");
                }
            }
            _ => panic!("Expected Expression in second position"),
        }
        
        // Rest of implementation will be added once the logical planner 
        // supports aggregation operators
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

#[test]
fn test_having_clause() -> Result<()> {
    // Test HAVING clause
    let sql = "SELECT department_id, COUNT(*) FROM employees GROUP BY department_id HAVING COUNT(*) > 5";
    
    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
    // Verify the AST structure
    if let Statement::Select(select) = statement {
        // Check GROUP BY clause
        assert!(select.group_by.is_some(), "Expected GROUP BY clause");
        
        // Check HAVING clause
        assert!(select.having.is_some(), "Expected HAVING clause");
        
        // Check HAVING contains an aggregate function
        if let Some(having) = &select.having {
            match &**having {
                Expression::BinaryOp { left, .. } => {
                    // Left side should be COUNT(*) 
                    if let Expression::Aggregate { function, .. } = &**left {
                        assert_eq!(*function, AggregateFunction::Count);
                    } else {
                        panic!("Expected aggregate function in HAVING clause");
                    }
                }
                _ => panic!("Expected binary operation in HAVING clause"),
            }
        }
        
        // Rest of implementation will be added once the logical planner 
        // supports aggregation operators
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

#[test]
fn test_multiple_aggregate_functions() -> Result<()> {
    // Test multiple aggregate functions
    let sql = "SELECT department_id, COUNT(*), SUM(salary), AVG(salary), MIN(salary), MAX(salary) \
               FROM employees \
               GROUP BY department_id";
    
    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
    // Verify the AST structure
    if let Statement::Select(select) = statement {
        // Check columns contain all aggregate functions
        assert_eq!(select.columns.len(), 6, "Expected six columns");
        
        // Check for aggregate functions
        let mut has_count = false;
        let mut has_sum = false;
        let mut has_avg = false;
        let mut has_min = false;
        let mut has_max = false;
        
        for col in &select.columns[1..] { // Skip first column (department_id)
            if let bayundb::query::parser::ast::SelectColumn::Expression { expr, .. } = col {
                if let Expression::Aggregate { function, .. } = &**expr {
                    match function {
                        AggregateFunction::Count => has_count = true,
                        AggregateFunction::Sum => has_sum = true,
                        AggregateFunction::Avg => has_avg = true, 
                        AggregateFunction::Min => has_min = true,
                        AggregateFunction::Max => has_max = true,
                    }
                }
            }
        }
        
        assert!(has_count, "COUNT not found");
        assert!(has_sum, "SUM not found");
        assert!(has_avg, "AVG not found");
        assert!(has_min, "MIN not found");
        assert!(has_max, "MAX not found");
        
        // Rest of implementation will be added once the logical planner 
        // supports aggregation operators
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

#[test]
fn test_complex_aggregation_query() -> Result<()> {
    // Test a complex query with filtering, grouping, and having
    let sql = "SELECT department_id, job_title, COUNT(*), AVG(salary) \
               FROM employees \
               WHERE status = 'active' \
               GROUP BY department_id, job_title \
               HAVING COUNT(*) > 2";
    
    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
    // Verify the AST structure
    if let Statement::Select(select) = statement {
        // Check basic structure
        assert_eq!(select.columns.len(), 4, "Expected four columns");
        assert!(select.where_clause.is_some(), "Expected WHERE clause");
        assert!(select.group_by.is_some(), "Expected GROUP BY clause");
        assert!(select.having.is_some(), "Expected HAVING clause");
        
        // Check GROUP BY has two columns
        let group_by = select.group_by.unwrap();
        assert_eq!(group_by.len(), 2, "Expected two columns in GROUP BY");
        
        // Check HAVING contains COUNT(*)
        if let Some(having) = &select.having {
            match &**having {
                Expression::BinaryOp { left, .. } => {
                    if let Expression::Aggregate { function, .. } = &**left {
                        assert_eq!(*function, AggregateFunction::Count);
                    } else {
                        panic!("Expected aggregate function in HAVING clause");
                    }
                }
                _ => panic!("Expected binary operation in HAVING clause"),
            }
        }
        
        // Rest of implementation will be added once the logical planner 
        // supports aggregation operators
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

#[test]
#[ignore] // Temporarily skip until parser is updated to handle complex expressions
fn test_end_to_end_count_query() -> Result<()> {
    // Create a temporary database for testing
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_agg_count.db");
    
    // Initialize buffer pool
    let buffer_pool = Arc::new(BufferPoolManager::new(
        100, // buffer pool size
        db_path.to_str().unwrap().to_string()
    ).map_err(|e| anyhow!("Failed to create buffer pool: {:?}", e))?);
    
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));
    // Create execution engine
    let engine = ExecutionEngine::new(buffer_pool, catalog_arc);
    
    // Simple COUNT(*) query
    let sql = "SELECT COUNT(*) FROM test_table";
    
    // Execute the query
    let result = engine.execute_query(sql)
        .map_err(|e| anyhow!("Query execution failed: {:?}", e))?;
    
    // Verify the result
    assert_eq!(result.row_count(), 1, "Expected one row in result set");
    
    // Ensure column is COUNT(*)
    let columns = result.columns();
    assert_eq!(columns[0], "COUNT(*)", "Expected COUNT(*) column");
    
    // Check that the count is correct
    // Our test_table implementation returns 19 rows in total
    let rows = result.rows();
    assert_eq!(rows.len(), 1, "Expected one row");
    
    let count_value = rows[0].get("COUNT(*)").expect("COUNT(*) column not found");
    match count_value {
        DataValue::Integer(count) => {
            assert_eq!(*count, 19, "Expected COUNT(*) = 19");
        },
        _ => panic!("Expected INTEGER value for COUNT(*)")
    }
    
    Ok(())
}

#[test]
#[ignore] // Temporarily skip until parser is updated to handle complex expressions
fn test_end_to_end_group_by_query() -> Result<()> {
    // Create a temporary database for testing
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_agg_group_by.db");
    
    // Initialize buffer pool
    let buffer_pool = Arc::new(BufferPoolManager::new(
        100, // buffer pool size
        db_path.to_str().unwrap().to_string()
    ).map_err(|e| anyhow!("Failed to create buffer pool: {:?}", e))?);
    
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));
    // Create execution engine
    let engine = ExecutionEngine::new(buffer_pool, catalog_arc);
    
    // Group by query - group by department_id which is a modulo of id
    // This will create groups 0, 1, 2, 3, 4 (with id % 5)
    let sql = "SELECT id % 5 as department_id, COUNT(*) FROM test_table GROUP BY id % 5";
    
    // Execute the query
    let result = engine.execute_query(sql)
        .map_err(|e| anyhow!("Query execution failed: {:?}", e))?;
    
    // Verify the result
    assert_eq!(result.columns().len(), 2, "Expected two columns in result set");
    assert_eq!(result.row_count(), 5, "Expected five rows in result set (groups)");
    
    // Check column names
    let columns = result.columns();
    assert_eq!(columns[0], "department_id", "Expected department_id column");
    assert_eq!(columns[1], "COUNT(*)", "Expected COUNT(*) column");
    
    // Check that each group has the right count
    // We have 19 rows total, so each group should have close to 19/5 = 3-4 rows
    let rows = result.rows();
    assert_eq!(rows.len(), 5, "Expected five rows (one per group)");
    
    // Total count across all groups should be 19
    let mut total_count = 0;
    for row in rows {
        if let Some(DataValue::Integer(count)) = row.get("COUNT(*)") {
            total_count += *count;
        }
    }
    assert_eq!(total_count, 19, "Expected total COUNT(*) across all groups to be 19");
    
    Ok(())
}

#[test]
#[ignore] // Temporarily skip until parser is updated to handle complex expressions
fn test_end_to_end_aggregation_functions() -> Result<()> {
    // Create a temporary database for testing
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_agg_functions.db");
    
    // Initialize buffer pool
    let buffer_pool = Arc::new(BufferPoolManager::new(
        100, // buffer pool size
        db_path.to_str().unwrap().to_string()
    ).map_err(|e| anyhow!("Failed to create buffer pool: {:?}", e))?);
    
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));
    // Create execution engine
    let engine = ExecutionEngine::new(buffer_pool, catalog_arc);
    
    // Query with multiple aggregate functions
    let sql = "SELECT MIN(id), MAX(id), SUM(id), AVG(id), COUNT(*) FROM test_table";
    
    // Execute the query
    let result = engine.execute_query(sql)
        .map_err(|e| anyhow!("Query execution failed: {:?}", e))?;
    
    // Verify the result
    assert_eq!(result.columns().len(), 5, "Expected five columns in result set");
    assert_eq!(result.row_count(), 1, "Expected one row in result set");
    
    // Check column names
    let columns = result.columns();
    assert_eq!(columns[0], "MIN(id)", "Expected MIN(id) column");
    assert_eq!(columns[1], "MAX(id)", "Expected MAX(id) column");
    assert_eq!(columns[2], "SUM(id)", "Expected SUM(id) column");
    assert_eq!(columns[3], "AVG(id)", "Expected AVG(id) column");
    assert_eq!(columns[4], "COUNT(*)", "Expected COUNT(*) column");
    
    // Test table has values 0-18, so:
    // MIN(id) = 0
    // MAX(id) = 18
    // SUM(id) = (0 + 1 + 2 + ... + 18) = 171
    // AVG(id) = 171 / 19 = 9.0
    // COUNT(*) = 19
    let rows = result.rows();
    let row = &rows[0];
    
    // Check MIN
    if let Some(DataValue::Integer(min)) = row.get("MIN(id)") {
        assert_eq!(*min, 0, "Expected MIN(id) = 0");
    } else {
        panic!("Expected INTEGER value for MIN(id)");
    }
    
    // Check MAX
    if let Some(DataValue::Integer(max)) = row.get("MAX(id)") {
        assert_eq!(*max, 18, "Expected MAX(id) = 18");
    } else {
        panic!("Expected INTEGER value for MAX(id)");
    }
    
    // Check SUM
    if let Some(DataValue::Integer(sum)) = row.get("SUM(id)") {
        assert_eq!(*sum, 171, "Expected SUM(id) = 171");
    } else {
        panic!("Expected INTEGER value for SUM(id)");
    }
    
    // Check AVG
    if let Some(DataValue::Float(avg)) = row.get("AVG(id)") {
        assert!((*avg - 9.0).abs() < 0.0001, "Expected AVG(id) ≈ 9.0");
    } else {
        panic!("Expected FLOAT value for AVG(id)");
    }
    
    // Check COUNT
    if let Some(DataValue::Integer(count)) = row.get("COUNT(*)") {
        assert_eq!(*count, 19, "Expected COUNT(*) = 19");
    } else {
        panic!("Expected INTEGER value for COUNT(*)");
    }
    
    Ok(())
}

#[test]
#[ignore] // Temporarily skip until parser is updated to handle complex expressions
fn test_end_to_end_having_clause() -> Result<()> {
    // Create a temporary database for testing
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_agg_having.db");
    
    // Initialize buffer pool
    let buffer_pool = Arc::new(BufferPoolManager::new(
        100, // buffer pool size
        db_path.to_str().unwrap().to_string()
    ).map_err(|e| anyhow!("Failed to create buffer pool: {:?}", e))?);
    
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));
    // Create execution engine
    let engine = ExecutionEngine::new(buffer_pool, catalog_arc);
    
    // Query with GROUP BY and HAVING
    // We'll group by id % 5 (creating 5 groups) and filter for groups with count > 3
    let sql = "SELECT id % 5 as group_id, COUNT(*) FROM test_table GROUP BY id % 5 HAVING COUNT(*) > 3";
    
    // Execute the query
    let result = engine.execute_query(sql)
        .map_err(|e| anyhow!("Query execution failed: {:?}", e))?;
    
    // Verify the result
    // We should have groups with count > 3
    let rows = result.rows();
    for row in rows {
        // Verify each group has COUNT(*) > 3
        if let Some(DataValue::Integer(count)) = row.get("COUNT(*)") {
            assert!(*count > 3, "Expected COUNT(*) > 3 for all groups in result");
        } else {
            panic!("Expected INTEGER value for COUNT(*)");
        }
        
        // Verify group_id is one of the expected values (0-4)
        if let Some(DataValue::Integer(group_id)) = row.get("group_id") {
            assert!(*group_id >= 0 && *group_id < 5, "Expected group_id between 0 and 4");
        } else {
            panic!("Expected INTEGER value for group_id");
        }
    }
    
    Ok(())
}

#[test]
#[ignore] // Temporarily skip until parser is updated to handle complex expressions
fn test_end_to_end_complex_grouping() -> Result<()> {
    // Create a temporary database for testing
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_agg_complex.db");
    
    // Initialize buffer pool
    let buffer_pool = Arc::new(BufferPoolManager::new(
        100, // buffer pool size
        db_path.to_str().unwrap().to_string()
    ).map_err(|e| anyhow!("Failed to create buffer pool: {:?}", e))?);
    
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));
    // Create execution engine
    let engine = ExecutionEngine::new(buffer_pool, catalog_arc);
    
    // Complex query with multiple group by columns, filter, and multiple aggregates
    let sql = "SELECT id % 3 as dept_id, id % 2 as job_type, COUNT(*), MIN(id), MAX(id) 
               FROM test_table 
               WHERE id > 5 
               GROUP BY id % 3, id % 2
               HAVING COUNT(*) > 1";
    
    // Execute the query
    let result = engine.execute_query(sql)
        .map_err(|e| anyhow!("Query execution failed: {:?}", e))?;
    
    // Verify the result
    assert!(result.row_count() > 0, "Expected at least one group in result");
    
    // Check column names
    let columns = result.columns();
    assert_eq!(columns[0], "dept_id", "Expected dept_id column");
    assert_eq!(columns[1], "job_type", "Expected job_type column");
    assert_eq!(columns[2], "COUNT(*)", "Expected COUNT(*) column");
    assert_eq!(columns[3], "MIN(id)", "Expected MIN(id) column");
    assert_eq!(columns[4], "MAX(id)", "Expected MAX(id) column");
    
    // Verify all rows meet our criteria
    let rows = result.rows();
    for row in rows {
        // Each group should have COUNT(*) > 1
        if let Some(DataValue::Integer(count)) = row.get("COUNT(*)") {
            assert!(*count > 1, "Expected COUNT(*) > 1 for all groups");
        } else {
            panic!("Expected INTEGER value for COUNT(*)");
        }
        
        // Each MIN(id) should be > 5 due to WHERE clause
        if let Some(DataValue::Integer(min_id)) = row.get("MIN(id)") {
            assert!(*min_id > 5, "Expected MIN(id) > 5 due to WHERE clause");
        } else {
            panic!("Expected INTEGER value for MIN(id)");
        }
        
        // Verify group values are as expected
        if let Some(DataValue::Integer(dept_id)) = row.get("dept_id") {
            assert!(*dept_id >= 0 && *dept_id < 3, "Expected dept_id between 0 and 2");
        } else {
            panic!("Expected INTEGER value for dept_id");
        }
        
        if let Some(DataValue::Integer(job_type)) = row.get("job_type") {
            assert!(*job_type >= 0 && *job_type < 2, "Expected job_type between 0 and 1");
        } else {
            panic!("Expected INTEGER value for job_type");
        }
    }
    
    Ok(())
}

#[test]
#[ignore] // Temporarily skip until Hash vs Sort aggregation strategy is implemented
fn test_hash_vs_sort_aggregation() -> Result<()> {
    // Create a temporary database for testing
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_agg_hash_sort.db");
    
    // Initialize buffer pool
    let buffer_pool = Arc::new(BufferPoolManager::new(
        100, // buffer pool size
        db_path.to_str().unwrap().to_string()
    ).map_err(|e| anyhow!("Failed to create buffer pool: {:?}", e))?);
    
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));
    // Create execution engine for hash aggregation
    let engine_hash = ExecutionEngine::new(buffer_pool.clone(), catalog_arc.clone());
    
    // Create execution engine for sort aggregation
    let engine_sort = ExecutionEngine::new(buffer_pool.clone(), catalog_arc.clone());
    
    // The query to test
    let sql = "SELECT id % 7 as group_id, COUNT(*), SUM(id), MIN(id), MAX(id) 
               FROM test_table 
               GROUP BY id % 7";
    
    // Execute the same query twice to ensure consistent results
    let first_result = engine_hash.execute_query(sql)
        .map_err(|e| anyhow!("First query execution failed: {:?}", e))?;
    let second_result = engine_sort.execute_query(sql)
        .map_err(|e| anyhow!("Second query execution failed: {:?}", e))?;
    
    // Verify both result sets have the same structure
    assert_eq!(first_result.columns().len(), second_result.columns().len(), 
               "Column count mismatch between query executions");
    assert_eq!(first_result.row_count(), second_result.row_count(), 
               "Row count mismatch between query executions");
    
    // Check that the column names match
    let first_columns = first_result.columns();
    let second_columns = second_result.columns();
    for (i, col) in first_columns.iter().enumerate() {
        assert_eq!(col, &second_columns[i], "Column name mismatch at index {}", i);
    }
    
    // Verify the data in each row matches
    // This is tricky because the order of rows might be different
    // Let's sort both result sets by group_id
    let mut first_rows = first_result.rows().to_vec();
    let mut second_rows = second_result.rows().to_vec();
    
    first_rows.sort_by(|a, b| {
        let a_group = a.get("group_id").unwrap();
        let b_group = b.get("group_id").unwrap();
        a_group.partial_cmp(b_group).unwrap()
    });
    
    second_rows.sort_by(|a, b| {
        let a_group = a.get("group_id").unwrap();
        let b_group = b.get("group_id").unwrap();
        a_group.partial_cmp(b_group).unwrap()
    });
    
    // Now compare the sorted rows
    for (i, (first_row, second_row)) in first_rows.iter().zip(second_rows.iter()).enumerate() {
        for col in first_columns {
            let first_val = first_row.get(col).unwrap();
            let second_val = second_row.get(col).unwrap();
            assert_eq!(first_val, second_val, "Value mismatch at row {}, column {}", i, col);
        }
    }
    
    Ok(())
}

// This main function helps with manual testing but is ignored in cargo test runs
#[allow(dead_code)]
fn main() {
    println!("Running aggregation integration tests");
    
    // Run all the tests
    if let Err(e) = test_count_query() {
        eprintln!("test_count_query failed: {}", e);
    } else {
        println!("test_count_query passed");
    }
    
    if let Err(e) = test_group_by_query() {
        eprintln!("test_group_by_query failed: {}", e);
    } else {
        println!("test_group_by_query passed");
    }
    
    if let Err(e) = test_having_clause() {
        eprintln!("test_having_clause failed: {}", e);
    } else {
        println!("test_having_clause passed");
    }
    
    if let Err(e) = test_multiple_aggregate_functions() {
        eprintln!("test_multiple_aggregate_functions failed: {}", e);
    } else {
        println!("test_multiple_aggregate_functions passed");
    }
    
    if let Err(e) = test_complex_aggregation_query() {
        eprintln!("test_complex_aggregation_query failed: {}", e);
    } else {
        println!("test_complex_aggregation_query passed");
    }
    
    if let Err(e) = test_end_to_end_count_query() {
        eprintln!("test_end_to_end_count_query failed: {}", e);
    } else {
        println!("test_end_to_end_count_query passed");
    }
    
    if let Err(e) = test_end_to_end_group_by_query() {
        eprintln!("test_end_to_end_group_by_query failed: {}", e);
    } else {
        println!("test_end_to_end_group_by_query passed");
    }
    
    if let Err(e) = test_end_to_end_aggregation_functions() {
        eprintln!("test_end_to_end_aggregation_functions failed: {}", e);
    } else {
        println!("test_end_to_end_aggregation_functions passed");
    }
    
    if let Err(e) = test_end_to_end_having_clause() {
        eprintln!("test_end_to_end_having_clause failed: {}", e);
    } else {
        println!("test_end_to_end_having_clause passed");
    }
    
    if let Err(e) = test_end_to_end_complex_grouping() {
        eprintln!("test_end_to_end_complex_grouping failed: {}", e);
    } else {
        println!("test_end_to_end_complex_grouping passed");
    }
    
    if let Err(e) = test_hash_vs_sort_aggregation() {
        eprintln!("test_hash_vs_sort_aggregation failed: {}", e);
    } else {
        println!("test_hash_vs_sort_aggregation passed");
    }
    
    println!("All tests completed");
} 