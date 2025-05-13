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
use bayundb::transaction::wal::log_manager::{LogManager, LogManagerConfig};
use bayundb::transaction::wal::log_buffer::LogBufferConfig;
use bayundb::transaction::concurrency::TransactionManager;

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
fn test_end_to_end_count_query() -> Result<()> {
    // Create a temporary database for testing
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_agg_count.db");
    let wal_dir = temp_dir.path().join("test_agg_count_wal");
    std::fs::create_dir_all(&wal_dir)?;
    let log_file_base_name = "agg_count_wal".to_string();
    
    // Initialize buffer pool
    let buffer_pool = Arc::new(BufferPoolManager::new(
        100, // buffer pool size
        db_path.to_str().unwrap()
    ).map_err(|e| anyhow!("Failed to create buffer pool: {:?}", e))?);
    
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));

    // Create LogManagerConfig
    let log_manager_config = LogManagerConfig {
        log_dir: wal_dir,
        log_file_base_name,
        max_log_file_size: 10 * 1024 * 1024, // 10MB
        buffer_config: LogBufferConfig::default(),
        force_sync: false,
    };
    let log_manager = Arc::new(LogManager::new(log_manager_config).unwrap());
    let transaction_manager = Arc::new(TransactionManager::new(log_manager.clone()));

    // Create execution engine
    let engine = ExecutionEngine::new(buffer_pool, catalog_arc, transaction_manager);
    
    // Create the test table and insert data
    engine.execute_query("CREATE TABLE test_table (id INT, name TEXT, price FLOAT)")
          .map_err(|e| anyhow!("CREATE TABLE failed: {:?}", e))?;

    for i in 0..19 {
        let insert_sql = format!("INSERT INTO test_table (id, name, price) VALUES ({}, 'Name_{}', {}.99)", i, i, i);
        engine.execute_query(&insert_sql)
              .map_err(|e| anyhow!("INSERT failed for id {}: {:?}", i, e))?;
    }

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
fn test_end_to_end_group_by_query() -> Result<()> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_agg_group_by.db");
    let wal_dir = temp_dir.path().join("test_agg_group_by_wal");
    std::fs::create_dir_all(&wal_dir)?;
    let log_file_base_name = "agg_group_by_wal".to_string();

    let buffer_pool = Arc::new(BufferPoolManager::new(
        100,
        db_path.to_str().unwrap()
    )?);
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));

    let log_manager_config = LogManagerConfig {
        log_dir: wal_dir,
        log_file_base_name,
        max_log_file_size: 10 * 1024 * 1024,
        buffer_config: LogBufferConfig::default(),
        force_sync: false,
    };
    let log_manager = Arc::new(LogManager::new(log_manager_config).unwrap());
    let transaction_manager = Arc::new(TransactionManager::new(log_manager.clone()));
    
    let engine = ExecutionEngine::new(buffer_pool, catalog_arc, transaction_manager);

    // Create the test table and insert data
    engine.execute_query("CREATE TABLE test_table (id INT, name TEXT, price FLOAT)")
          .map_err(|e| anyhow!("CREATE TABLE failed: {:?}", e))?;

    for i in 0..19 {
        let insert_sql = format!("INSERT INTO test_table (id, name, price) VALUES ({}, 'Name_{}', {}.99)", i, i, i);
        engine.execute_query(&insert_sql)
              .map_err(|e| anyhow!("INSERT failed for id {}: {:?}", i, e))?;
    }

    // Create a test table
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
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_agg_functions.db");
    let wal_dir = temp_dir.path().join("test_agg_functions_wal");
    std::fs::create_dir_all(&wal_dir)?;
    let log_file_base_name = "agg_functions_wal".to_string();

    let buffer_pool = Arc::new(BufferPoolManager::new(
        100,
        db_path.to_str().unwrap()
    )?);
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));

    let log_manager_config = LogManagerConfig {
        log_dir: wal_dir,
        log_file_base_name,
        max_log_file_size: 10 * 1024 * 1024,
        buffer_config: LogBufferConfig::default(),
        force_sync: false,
    };
    let log_manager = Arc::new(LogManager::new(log_manager_config).unwrap());
    let transaction_manager = Arc::new(TransactionManager::new(log_manager.clone()));

    let engine = ExecutionEngine::new(buffer_pool, catalog_arc, transaction_manager);

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
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_agg_having.db");
    let wal_dir = temp_dir.path().join("test_agg_having_wal");
    std::fs::create_dir_all(&wal_dir)?;
    let log_file_base_name = "agg_having_wal".to_string();

    let buffer_pool = Arc::new(BufferPoolManager::new(
        100,
        db_path.to_str().unwrap()
    )?);
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));

    let log_manager_config = LogManagerConfig {
        log_dir: wal_dir,
        log_file_base_name,
        max_log_file_size: 10 * 1024 * 1024,
        buffer_config: LogBufferConfig::default(),
        force_sync: false,
    };
    let log_manager = Arc::new(LogManager::new(log_manager_config).unwrap());
    let transaction_manager = Arc::new(TransactionManager::new(log_manager.clone()));

    let engine = ExecutionEngine::new(buffer_pool, catalog_arc, transaction_manager);

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
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_agg_complex.db");
    let wal_dir = temp_dir.path().join("test_agg_complex_wal");
    std::fs::create_dir_all(&wal_dir)?;
    let log_file_base_name = "agg_complex_wal".to_string();

    let buffer_pool = Arc::new(BufferPoolManager::new(
        100,
        db_path.to_str().unwrap()
    )?);
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));

    let log_manager_config = LogManagerConfig {
        log_dir: wal_dir,
        log_file_base_name,
        max_log_file_size: 10 * 1024 * 1024,
        buffer_config: LogBufferConfig::default(),
        force_sync: false,
    };
    let log_manager = Arc::new(LogManager::new(log_manager_config).unwrap());
    let transaction_manager = Arc::new(TransactionManager::new(log_manager.clone()));
    
    let engine = ExecutionEngine::new(buffer_pool, catalog_arc, transaction_manager);

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

// Ensure all tests are defined within the `tests` module or marked with `#[test]`
#[cfg(test)]
mod tests {
    use super::*; 
    use bayundb::query::executor::result::{DataValue, Row}; 
    use std::path::PathBuf;
    use tempfile::{NamedTempFile, tempdir}; 
    use bayundb::transaction::wal::log_manager::{LogManager, LogManagerConfig}; 
    use bayundb::transaction::wal::log_buffer::LogBufferConfig; 
    use bayundb::storage::buffer::BufferPoolManager; 
    use std::sync::{Arc, RwLock};
    use bayundb::catalog::Catalog;
    use bayundb::transaction::concurrency::TransactionManager;
    use bayundb::query::executor::engine::ExecutionEngine;
    use anyhow::{Result, anyhow};

    // Helper function to set up initial tables for aggregation tests
    fn setup_tables(engine: &ExecutionEngine) -> Result<(), String> {
        let queries = vec![
            "CREATE TABLE products (id INT, name TEXT, category TEXT, price FLOAT);",
            "INSERT INTO products VALUES (1, 'Laptop', 'Electronics', 1200.00);",
            "INSERT INTO products VALUES (2, 'Mouse', 'Electronics', 25.00);",
            "INSERT INTO products VALUES (3, 'Keyboard', 'Electronics', 75.00);",
            "INSERT INTO products VALUES (4, 'Desk Chair', 'Furniture', 150.00);",
            "INSERT INTO products VALUES (5, 'Monitor', 'Electronics', 300.00);",
            "INSERT INTO products VALUES (6, 'Desk Lamp', 'Furniture', 35.00);",
            "INSERT INTO products VALUES (7, 'USB Cable', 'Electronics', 10.00);",
            "INSERT INTO products VALUES (8, 'Webcam', 'Electronics', 50.00);",
            "INSERT INTO products VALUES (9, 'Office Desk', 'Furniture', 250.00);",
            "INSERT INTO products VALUES (10, 'Mouse Pad', 'Accessories', 5.00);",
            "INSERT INTO products VALUES (11, 'External HDD', 'Electronics', 80.00);",
            "INSERT INTO products VALUES (12, 'Speakers', 'Electronics', 40.00);",
            "INSERT INTO products VALUES (13, 'Coffee Mug', 'Accessories', 15.00);",
            "INSERT INTO products VALUES (14, 'Pen Holder', 'Accessories', NULL);" // Item with NULL price
        ];

        for query in queries {
            engine.execute_query(query).map_err(|e| format!("Failed to execute query '{}': {:?}", query, e))?;
        }
        Ok(())
    }

    // Helper to run an aggregation query and verify its results
    fn run_aggregation_query_test(
        engine: &ExecutionEngine,
        test_name: &str,
        query: &str,
        expected_columns: Vec<&str>,
        expected_rows: Vec<Vec<DataValue>>,
    ) -> Result<(), String> {
        let result_set = engine.execute_query(query).map_err(|e| format!("Test '{}': Query execution failed for '{}': {:?}", test_name, query, e))?;

        // Check columns first
        let actual_columns: Vec<String> = result_set.columns().iter().map(|s| s.to_string()).collect();
        let expected_columns_str: Vec<String> = expected_columns.iter().map(|s| s.to_string()).collect();
        assert_eq!(actual_columns, expected_columns_str, "Column mismatch for {}. Expected: {:?}, Got: {:?}", test_name, expected_columns_str, actual_columns);


        // Check row count
        assert_eq!(result_set.rows().len(), expected_rows.len(), "Row count mismatch for {}. Expected: {}, Got: {}", test_name, expected_rows.len(), result_set.rows().len());

        // Check row values
        // Sort both actual and expected rows for consistent comparison, assuming order doesn't matter unless explicitly tested with ORDER BY
        // Use the first column for sorting if available and comparable, otherwise compare as is.
        let mut actual_rows_sorted: Vec<Row> = result_set.rows().to_vec(); // Ensure this uses .to_vec()
        let mut expected_rows_sorted = expected_rows.clone(); // Clone to sort

        // Simple sort based on first column's String representation for comparison stability
        let sort_key = |row: &Vec<DataValue>| -> String {
            row.get(0).map_or_else(|| "".to_string(), |v| format!("{:?}", v))
        };

        // Simplify the sort for actual_rows_sorted (Vec<Row>)
        actual_rows_sorted.sort_by(|a, b| { // a: &Row, b: &Row
            // Extract the first value's string representation directly for sorting
            let a_key = a.values().get(0).map_or_else(|| "".to_string(), |v| format!("{:?}", v));
            let b_key = b.values().get(0).map_or_else(|| "".to_string(), |v| format!("{:?}", v));
            a_key.cmp(&b_key)
        });

        // Sort expected rows using the helper function
        expected_rows_sorted.sort_by(|a, b| sort_key(a).cmp(&sort_key(b)));


        for (i, expected_row_values) in expected_rows_sorted.iter().enumerate() {
            let actual_row = actual_rows_sorted.get(i).ok_or_else(|| format!("Sorted row index {} out of bounds for {}", i, test_name))?;
            let actual_values_in_row = actual_row.values(); // This is Vec<&DataValue>
            assert_eq!(actual_values_in_row.len(), expected_row_values.len(), "Mismatch in value count for sorted row {} in {}", i, test_name);
            for (j, expected_val) in expected_row_values.iter().enumerate() {
                let actual_val_ref = actual_values_in_row.get(j).ok_or_else(|| format!("Value index {} out of bounds for sorted row {} in {}", j, i, test_name))?;
                // Dereference the reference before comparison
                assert_eq!(**actual_val_ref, *expected_val, "Mismatch in value at sorted row {}, col {} ({}) for {}. Expected: {:?}, Got: {:?}. Query: {}", i, j, actual_columns[j], test_name, *expected_val, **actual_val_ref, query);
            }
        }
        Ok(())
    }

    // Helper to setup engine and tables for actual test functions
    fn setup_engine_and_tables_for_test() -> Result<ExecutionEngine, String> {
        let temp_log_dir = tempdir().map_err(|e| format!("Failed to create temp log dir: {}", e))?;
        let log_config = LogManagerConfig {
            log_dir: PathBuf::from(temp_log_dir.path()),
            log_file_base_name: "test_wal".to_string(),
            max_log_file_size: 1024 * 10, // Smaller for tests
            buffer_config: LogBufferConfig::default(),
            force_sync: false,
        };
        let log_manager = Arc::new(LogManager::new(log_config).map_err(|e| format!("Failed to create LogManager: {}", e))?);

        let temp_db_file = NamedTempFile::new().map_err(|e| format!("Failed to create temp db file: {}", e))?;
        let db_path = temp_db_file.path().to_str().unwrap().to_string();

        let buffer_pool = Arc::new(BufferPoolManager::new_with_wal(100, db_path, log_manager.clone()).map_err(|e| format!("Failed to create BufferPoolManager: {}", e))?);
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        let transaction_manager = Arc::new(TransactionManager::new(log_manager.clone()));
        let engine = ExecutionEngine::new(buffer_pool, catalog, transaction_manager);

        setup_tables(&engine)?; // Ensure no super:: here
        Ok(engine)
    }

    #[test]
    fn test_count_query() {
        let engine = setup_engine_and_tables_for_test().expect("Test setup failed for count_query");
        run_aggregation_query_test( // REMOVE super::
            &engine,
            "COUNT_ALL",
            "SELECT COUNT(*) FROM products;",
            vec!["COUNT(*)"],
            vec![vec![DataValue::Integer(14)]]
        ).expect("COUNT_ALL test failed");
    }

    #[test]
    fn test_group_by_query() {
        let engine = setup_engine_and_tables_for_test().expect("Test setup failed for group_by_query");
        run_aggregation_query_test( // REMOVE super::
            &engine,
            "GROUP_BY_CATEGORY_COUNT",
            "SELECT category, COUNT(*) FROM products GROUP BY category ORDER BY category;",
            vec!["category", "COUNT(*)"],
            vec![
                vec![DataValue::Text("Accessories".to_string()), DataValue::Integer(3)],
                vec![DataValue::Text("Electronics".to_string()), DataValue::Integer(8)],
                vec![DataValue::Text("Furniture".to_string()), DataValue::Integer(3)],
            ]
        ).expect("GROUP_BY_CATEGORY_COUNT test failed");

        run_aggregation_query_test( // REMOVE super::
            &engine,
            "GROUP_BY_CATEGORY_SUM_PRICE",
            "SELECT category, SUM(price) FROM products GROUP BY category ORDER BY category;",
            vec!["category", "SUM(price)"],
            vec![
                vec![DataValue::Text("Accessories".to_string()), DataValue::Float(20.00)], // 5 + 15 (NULL price ignored)
                vec![DataValue::Text("Electronics".to_string()), DataValue::Float(1780.00)], // Sum of all electronics
                vec![DataValue::Text("Furniture".to_string()), DataValue::Float(435.00)], // 150 + 35 + 250
            ]
        ).expect("GROUP_BY_CATEGORY_SUM_PRICE test failed");
    }

    #[test]
    fn test_multiple_aggregate_functions() {
        let engine = setup_engine_and_tables_for_test().expect("Test setup failed for multiple_aggregate_functions");
        run_aggregation_query_test( // REMOVE super::
            &engine,
            "MULTIPLE_AGGREGATES_PER_CATEGORY",
            "SELECT category, COUNT(*), SUM(price), AVG(price), MIN(price), MAX(price) FROM products GROUP BY category ORDER BY category;",
            vec!["category", "COUNT(*)", "SUM(price)", "AVG(price)", "MIN(price)", "MAX(price)"],
            // Expected values need careful calculation, especially AVG
            vec![
                vec![DataValue::Text("Accessories".to_string()), DataValue::Integer(3), DataValue::Float(20.00), DataValue::Float(10.00), DataValue::Float(5.00), DataValue::Float(15.00)],
                vec![DataValue::Text("Electronics".to_string()), DataValue::Integer(8), DataValue::Float(1780.00), DataValue::Float(222.50), DataValue::Float(10.00), DataValue::Float(1200.00)],
                vec![DataValue::Text("Furniture".to_string()), DataValue::Integer(3), DataValue::Float(435.00), DataValue::Float(145.00), DataValue::Float(35.00), DataValue::Float(250.00)],
            ]
        ).expect("MULTIPLE_AGGREGATES_PER_CATEGORY test failed");
    }

    #[test]
    fn test_having_clause() {
        let engine = setup_engine_and_tables_for_test().expect("Test setup failed for having_clause");
        run_aggregation_query_test( // REMOVE super::
            &engine,
            "HAVING_CATEGORY_COUNT_GT_2",
            "SELECT category, COUNT(*) FROM products GROUP BY category HAVING COUNT(*) > 2 ORDER BY category;",
            vec!["category", "COUNT(*)"],
            vec![
                vec![DataValue::Text("Accessories".to_string()), DataValue::Integer(3)],
                vec![DataValue::Text("Electronics".to_string()), DataValue::Integer(8)],
                vec![DataValue::Text("Furniture".to_string()), DataValue::Integer(3)],
            ]
        ).expect("HAVING_CATEGORY_COUNT_GT_2 test failed");

        run_aggregation_query_test( // REMOVE super::
            &engine,
            "HAVING_SUM_PRICE_GT_500",
            "SELECT category, SUM(price) FROM products GROUP BY category HAVING SUM(price) > 500.0 ORDER BY category;",
            vec!["category", "SUM(price)"],
            vec![
                vec![DataValue::Text("Electronics".to_string()), DataValue::Float(1780.00)]
            ]
        ).expect("HAVING_SUM_PRICE_GT_500 test failed");
    }

    #[test]
    fn test_complex_aggregation_query() {
        let engine = setup_engine_and_tables_for_test().expect("Test setup failed for complex_aggregation_query");
        run_aggregation_query_test(
            &engine,
            "COMPLEX_AGGREGATION_WITH_HAVING_AND_ORDER",
            "SELECT category, AVG(price) AS avg_price FROM products WHERE price > 10.00 GROUP BY category HAVING COUNT(*) >= 2 ORDER BY avg_price DESC;",
            vec!["category", "avg_price"],
            vec![
                vec![DataValue::Text("Electronics".to_string()), DataValue::Float(1770.0/7.0)],
                vec![DataValue::Text("Furniture".to_string()), DataValue::Float(145.0)],
            ]
        ).expect("COMPLEX_AGGREGATION_WITH_HAVING_AND_ORDER test failed");
    }

    // Moved test from top-level
    #[test]
    #[ignore] // Temporarily skip until Hash vs Sort aggregation strategy is implemented
    fn test_hash_vs_sort_aggregation_impl() -> Result<()> {
        let temp_dir = tempdir()?;
        let db_path = temp_dir.path().join("test_agg_hash_sort.db");
        let wal_dir = temp_dir.path().join("test_agg_hash_sort_wal");
        std::fs::create_dir_all(&wal_dir)?;
        let log_file_base_name_prefix = "agg_hash_sort_wal".to_string();

        let buffer_pool = Arc::new(BufferPoolManager::new(
            100,
            db_path.to_str().unwrap()
        )?);
        let catalog_arc = Arc::new(RwLock::new(Catalog::new()));

        // Config for hash aggregation engine
        let log_manager_config_hash = LogManagerConfig {
            log_dir: wal_dir.clone(), // Clone for this specific config
            log_file_base_name: format!("{}_hash", log_file_base_name_prefix),
            max_log_file_size: 10 * 1024 * 1024,
            buffer_config: LogBufferConfig::default(),
            force_sync: false,
        };
        let log_manager_hash = Arc::new(LogManager::new(log_manager_config_hash).unwrap());
        let transaction_manager_hash = Arc::new(TransactionManager::new(log_manager_hash.clone()));
        let engine_hash = ExecutionEngine::new(buffer_pool.clone(), catalog_arc.clone(), transaction_manager_hash);

        // Config for sort aggregation engine
        let log_manager_config_sort = LogManagerConfig {
            log_dir: wal_dir, // Can reuse wal_dir if LogManager handles distinct file names
            log_file_base_name: format!("{}_sort", log_file_base_name_prefix),
            max_log_file_size: 10 * 1024 * 1024,
            buffer_config: LogBufferConfig::default(),
            force_sync: false,
        };
        let log_manager_sort = Arc::new(LogManager::new(log_manager_config_sort).unwrap());
        let transaction_manager_sort = Arc::new(TransactionManager::new(log_manager_sort.clone()));
        let engine_sort = ExecutionEngine::new(buffer_pool.clone(), catalog_arc.clone(), transaction_manager_sort);

        // Create a test table for hash aggregation
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
        let mut actual_rows_sorted: Vec<Row> = first_result.rows().to_vec(); // Use .to_vec() here too
        let mut expected_rows_sorted = second_result.rows().to_vec(); // Use .to_vec() here too

        actual_rows_sorted.sort_by(|a, b| {
            let a_group = a.get("group_id").unwrap();
            let b_group = b.get("group_id").unwrap();
            a_group.partial_cmp(b_group).unwrap_or(std::cmp::Ordering::Equal) // Handle potential NaN etc.
        });

        expected_rows_sorted.sort_by(|a, b| {
            let a_group = a.get("group_id").unwrap();
            let b_group = b.get("group_id").unwrap();
            a_group.partial_cmp(b_group).unwrap_or(std::cmp::Ordering::Equal) // Handle potential NaN etc.
        });

        // Now compare the sorted rows
        for (i, (first_row, second_row)) in actual_rows_sorted.iter().zip(expected_rows_sorted.iter()).enumerate() {
            for col in first_columns {
                let first_val = first_row.get(col).unwrap();
                let second_val = second_row.get(col).unwrap();
                assert_eq!(first_val, second_val, "Value mismatch at row {}, column {}", i, col);
            }
        }

        Ok(())
    }
} // This is the end of mod tests 