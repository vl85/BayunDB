use anyhow::{Result, anyhow};
use bayundb::query::executor::result::{DataValue, Row};
use bayundb::query::executor::operators::{create_filter, create_table_scan};
use bayundb::query::executor::engine::ExecutionEngine;
use bayundb::catalog::{Catalog, Table, Column, DataType};
use std::sync::Arc;
use tempfile::tempdir;
use bayundb::storage::buffer::BufferPoolManager;
use bayundb::storage::page::PageManager;
use bayundb::query::parser::ast::{Expression, ColumnReference, Operator, Value};
use std::sync::RwLock;
use bayundb::transaction::concurrency::transaction_manager::TransactionManager;
use bayundb::transaction::wal::log_manager::{LogManager, LogManagerConfig};
use bayundb::transaction::wal::log_buffer::LogBufferConfig;
use std::path::PathBuf;

// Declare the common module for test utilities using a path attribute
#[path = "../common/mod.rs"]
mod common;
use common::insert_test_data;

/// Helper function to create the a table schema and an empty table
fn create_test_table_schema(catalog_arc: &Arc<RwLock<Catalog>>, table_name: &str) -> Result<()> {
    let catalog = catalog_arc.write().unwrap();
    if catalog.get_table(table_name).is_none() {
        let columns = vec![
            Column::new("id".to_string(), DataType::Integer, false, true, None), // PK
            Column::new("name".to_string(), DataType::Text, false, false, None),
            Column::new("age".to_string(), DataType::Integer, false, false, None),
            Column::new("active".to_string(), DataType::Boolean, false, false, None),
        ];
        let table = Table::new(table_name.to_string(), columns);
        catalog.create_table(table).map_err(|e| anyhow!(e))?; // Map error for anyhow
    }
    Ok(())
}

/// Generates data for a generic test table schema (id, name, age, active)
fn generate_data_for_generic_table(num_rows: i64) -> Vec<Vec<DataValue>> {
    let mut data = Vec::new();
    for i in 0..num_rows {
        let mut values_in_order: Vec<DataValue> = Vec::new();
        values_in_order.push(DataValue::Integer(i));
        values_in_order.push(DataValue::Text(format!("User {}", i)));
        values_in_order.push(DataValue::Integer(20 + (i % 50)));
        values_in_order.push(DataValue::Boolean(i % 2 == 0));
        data.push(values_in_order);
    }
    data
}

/// Test to verify actual result rows from query execution with filter
#[test]
fn test_result_rows() -> Result<()> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_db_result.db");
    let buffer_pool = Arc::new(BufferPoolManager::new(100, db_path.to_str().unwrap())?);
    let page_manager = PageManager::new();
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));

    let table_name = format!("test_table_result_{}", rand::random::<u32>());

    create_test_table_schema(&catalog_arc, &table_name)?;
    let test_data = generate_data_for_generic_table(19);
    insert_test_data(&table_name, test_data, &buffer_pool, &page_manager, &catalog_arc)?;

    let scan_op = create_table_scan(&table_name, &table_name, buffer_pool.clone(), catalog_arc.clone())
        .map_err(|e| anyhow!(format!("Failed to create scan operator for table {}: {:?}", table_name, e)))?;
    
    let predicate_expr = Expression::BinaryOp {
        left: Box::new(Expression::Column(ColumnReference {
            table: None,
            name: "id".to_string(),
        })),
        op: Operator::LessThan,
        right: Box::new(Expression::Literal(Value::Integer(10))),
    };
    let filter_op = create_filter(scan_op, predicate_expr, table_name.clone())
        .map_err(|e| anyhow!("Failed to create filter operator: {:?}", e))?;
    
    let mut op = filter_op.lock().map_err(|e| anyhow!("Failed to lock operator: {}", e))?;
    op.init().map_err(|e| anyhow!("Failed to initialize operator: {:?}", e))?;
    
    let mut result_rows: Vec<Row> = Vec::new();
    {
        while let Some(row) = op.next().map_err(|e| anyhow!("Error getting next row: {:?}", e))? {
            result_rows.push(row);
        }
        
        op.close().map_err(|e| anyhow!("Failed to close operator: {:?}", e))?;
    }
    
    assert_eq!(result_rows.len(), 10, "Expected 10 rows for id < 10");
    for row in result_rows {
        if let Some(DataValue::Integer(id)) = row.get(&format!("{}.id", table_name)) {
            assert!(*id < 10, "Expected id < 10, got {}", id);
        } else {
            panic!("Expected INTEGER column '{}.id' in filtered results", table_name);
        }
    }
    Ok(())
}

/// Test CREATE TABLE execution - this test is commented out since CREATE TABLE
/// is not yet implemented in the parser, but this test framework will be useful
/// once that functionality is added.
#[test]
fn test_create_table_execution() -> Result<()> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_create_db.db");
    
    // Create a separate directory for WAL files if it doesn't exist
    let wal_artifacts_dir = temp_dir.path().join("test_create_db_wal_artifacts");
    std::fs::create_dir_all(&wal_artifacts_dir)?;
    let log_file_base_name = "test_create_table_wal".to_string();

    let buffer_pool = Arc::new(BufferPoolManager::new(
        100,
        db_path.to_str().unwrap()
    )?);
    
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));

    // Create LogManagerConfig
    let log_manager_config = LogManagerConfig {
        log_dir: wal_artifacts_dir, // Use the dedicated WAL directory
        log_file_base_name,
        max_log_file_size: 10 * 1024 * 1024, // 10MB
        buffer_config: LogBufferConfig::default(),
        force_sync: false,
    };
    let log_manager = Arc::new(LogManager::new(log_manager_config).unwrap());
    let transaction_manager = Arc::new(TransactionManager::new(log_manager.clone()));

    let _engine = ExecutionEngine::new(buffer_pool.clone(), catalog_arc, transaction_manager);
    
    Ok(())
}

/// Test to verify rows with different filter condition
#[test]
fn test_different_filter_condition() -> Result<()> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_db_filter.db");
    let buffer_pool = Arc::new(BufferPoolManager::new(100, db_path.to_str().unwrap())?);
    let page_manager = PageManager::new();
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));

    let table_name = format!("test_table_filter_{}", rand::random::<u32>());

    create_test_table_schema(&catalog_arc, &table_name)?;
    let test_data = generate_data_for_generic_table(19);
    insert_test_data(&table_name, test_data, &buffer_pool, &page_manager, &catalog_arc)?;

    let scan_op = create_table_scan(&table_name, &table_name, buffer_pool.clone(), catalog_arc.clone())
        .map_err(|e| anyhow!(format!("Failed to create scan operator for table {}: {:?}", table_name, e)))?;
    
    let predicate_expr = Expression::BinaryOp {
        left: Box::new(Expression::Column(ColumnReference {
            table: None,
            name: "id".to_string(),
        })),
        op: Operator::GreaterThan,
        right: Box::new(Expression::Literal(Value::Integer(15))),
    };
    let filter_op = create_filter(scan_op, predicate_expr, table_name.clone())
        .map_err(|e| anyhow!("Failed to create filter operator: {:?}", e))?;
    
    let mut op = filter_op.lock().map_err(|e| anyhow!("Failed to lock operator: {}", e))?;
    op.init().map_err(|e| anyhow!("Failed to initialize operator: {:?}", e))?;
    
    let mut result_rows: Vec<Row> = Vec::new();
    {
        while let Some(row) = op.next().map_err(|e| anyhow!("Error getting next row: {:?}", e))? {
            result_rows.push(row);
        }
        
        op.close().map_err(|e| anyhow!("Failed to close operator: {:?}", e))?;
    }
    
    assert_eq!(result_rows.len(), 3, "Expected 3 rows for id > 15");
    for row in result_rows {
        if let Some(DataValue::Integer(id)) = row.get(&format!("{}.id", table_name)) {
            assert!(*id > 15, "Expected id > 15, got {}", id);
        } else {
            panic!("Expected INTEGER column '{}.id' in filtered results", table_name);
        }
    }
    Ok(())
}

/// Test to verify scanning all rows using ExecutionEngine for INSERT and SELECT
#[test]
fn test_scan_all_rows_with_engine() -> Result<()> {
    let temp_dir_main = tempdir()?; 
    let db_path = temp_dir_main.path().join("test_scan_engine.db");

    let temp_dir_log = tempdir()?; 
    let log_path = temp_dir_log.path();
    let log_manager_config = LogManagerConfig {
        log_dir: PathBuf::from(log_path),
        log_file_base_name: "test_wal_scan".to_string(),
        max_log_file_size: 1024 * 1024, 
        buffer_config: LogBufferConfig::default(),
        force_sync: false, 
    };
    let log_manager = Arc::new(LogManager::new(log_manager_config)?);
    
    let buffer_pool = Arc::new(BufferPoolManager::new_with_wal(
        100, 
        db_path.to_str().unwrap(), 
        log_manager.clone()
    )?);
    
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));
    let transaction_manager = Arc::new(TransactionManager::new(log_manager.clone()));
    
    let engine = ExecutionEngine::new(buffer_pool.clone(), catalog_arc.clone(), transaction_manager.clone());

    let table_name = format!("test_scan_engine_{}", rand::random::<u32>());

    let create_query = format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, name TEXT, age INT, active BOOLEAN);",
        table_name
    );
    let create_result = engine.execute_query(&create_query)?;
    assert!(create_result.rows().first().unwrap().get("status").unwrap().to_string().contains("created successfully"));

    let num_rows_to_insert = 5;
    for i in 0..num_rows_to_insert {
        let name_val = format!("User {}", i);
        let age_val = 20 + i;
        let active_val = if i % 2 == 0 { "true" } else { "false" };
        let insert_query = format!(
            "INSERT INTO {} (id, name, age, active) VALUES ({}, '{}', {}, {});",
            table_name, i, name_val, age_val, active_val
        );
        let insert_result = engine.execute_query(&insert_query)?;
        assert!(insert_result.rows().first().unwrap().get("status").unwrap().to_string().contains("INSERT into"));
        assert!(insert_result.rows().first().unwrap().get("status").unwrap().to_string().contains("successful"));
    }

    let select_query_unordered = format!("SELECT id, name, age, active FROM {};", table_name);
    let result_set = engine.execute_query(&select_query_unordered)?;
    let mut found_rows: Vec<Row> = result_set.rows().to_vec();
    
    found_rows.sort_by(|a, b| {
        let id_a = match a.get("id") { Some(DataValue::Integer(val)) => *val, _ => panic!("Row missing 'id' or not an integer: {:?}", a) };
        let id_b = match b.get("id") { Some(DataValue::Integer(val)) => *val, _ => panic!("Row missing 'id' or not an integer: {:?}", b) };
        id_a.cmp(&id_b)
    });

    assert_eq!(found_rows.len(), num_rows_to_insert as usize, "Expected {} rows from SELECT", num_rows_to_insert);

    for i in 0..num_rows_to_insert {
        let row = &found_rows[i as usize];
        match row.get("id") { Some(DataValue::Integer(id_val)) => assert_eq!(*id_val, i as i64, "ID mismatch for row {}", i), _ => panic!("Missing or incorrect type for 'id' in row {}: {:?}", i, row) };
        let expected_name = format!("User {}", i);
        match row.get("name") { Some(DataValue::Text(name_val)) => assert_eq!(name_val, &expected_name, "Name mismatch for row {}", i), _ => panic!("Missing or incorrect type for 'name' in row {}: {:?}", i, row) };
        let expected_age = 20 + i;
        match row.get("age") { Some(DataValue::Integer(age_val)) => assert_eq!(*age_val, expected_age as i64, "Age mismatch for row {}", i), _ => panic!("Missing or incorrect type for 'age' in row {}: {:?}", i, row) };
        let expected_active = i % 2 == 0;
        match row.get("active") { Some(DataValue::Boolean(active_val)) => assert_eq!(*active_val, expected_active, "Active mismatch for row {}", i), _ => panic!("Missing or incorrect type for 'active' in row {}: {:?}", i, row) };
    }
    
    Ok(())
}

/// Test to verify basic UPDATE statement execution
#[test]
fn test_update_rows_with_engine() -> Result<()> {
    let temp_dir_main = tempdir()?;
    let db_path = temp_dir_main.path().join("test_update_engine.db");

    let temp_dir_log = tempdir()?;
    let log_path = temp_dir_log.path();
    let log_manager_config = LogManagerConfig {
        log_dir: PathBuf::from(log_path),
        log_file_base_name: "test_wal_update".to_string(),
        max_log_file_size: 1024 * 1024,
        buffer_config: LogBufferConfig::default(),
        force_sync: false,
    };
    let log_manager = Arc::new(LogManager::new(log_manager_config)?);

    let buffer_pool = Arc::new(BufferPoolManager::new_with_wal(
        100,
        db_path.to_str().unwrap(),
        log_manager.clone(),
    )?);

    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));
    let transaction_manager = Arc::new(TransactionManager::new(log_manager.clone()));

    let engine = ExecutionEngine::new(buffer_pool.clone(), catalog_arc.clone(), transaction_manager.clone());

    let table_name = format!("test_update_table_{}", rand::random::<u32>());

    let create_query = format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, name TEXT, age INT, city TEXT);",
        table_name
    );
    engine.execute_query(&create_query)?;

    // Insert initial data
    let initial_data = vec![
        (1, "Alice", 30, "New York"),
        (2, "Bob", 24, "London"),
        (3, "Charlie", 35, "Paris"),
        (4, "David", 28, "New York"),
    ];

    for (id, name, age, city) in initial_data {
        let insert_query = format!(
            "INSERT INTO {} (id, name, age, city) VALUES ({}, '{}', {}, '{}');",
            table_name, id, name, age, city
        );
        engine.execute_query(&insert_query)?;
    }

    // Update rows where city is "New York"
    let update_query = format!(
        "UPDATE {} SET age = age + 1, city = 'Brooklyn' WHERE city = 'New York';",
        table_name
    );
    let update_result = engine.execute_query(&update_query)?;
    // Assuming the engine returns a status message for UPDATE, like "UPDATE X rows affected"
    // For now, let's just check it doesn't error, specific row count can be asserted later if available
    assert!(update_result.rows().first().unwrap().get("status").unwrap().to_string().contains("UPDATE"));
    // TODO: Assert number of rows affected if the engine provides this info.

    // Verify the updated data
    let select_query = format!("SELECT id, name, age, city FROM {};", table_name);
    let result_set = engine.execute_query(&select_query)?;
    let mut updated_rows: Vec<Row> = result_set.rows().to_vec();
    updated_rows.sort_by_key(|row| match row.get("id") { Some(DataValue::Integer(val)) => *val, _ => 0 });

    assert_eq!(updated_rows.len(), 4, "Should still have 4 rows after update");

    // Expected data after update
    // Alice (id 1): age 30 -> 31, city New York -> Brooklyn
    // Bob (id 2): age 24, city London (unchanged)
    // Charlie (id 3): age 35, city Paris (unchanged)
    // David (id 4): age 28 -> 29, city New York -> Brooklyn

    for row in updated_rows {
        let id = match row.get("id") { Some(DataValue::Integer(val)) => *val, _ => panic!("Missing id") };
        let name = match row.get("name") { Some(DataValue::Text(val)) => val.clone(), _ => panic!("Missing name") };
        let age = match row.get("age") { Some(DataValue::Integer(val)) => *val, _ => panic!("Missing age") };
        let city = match row.get("city") { Some(DataValue::Text(val)) => val.clone(), _ => panic!("Missing city") };

        match id {
            1 => {
                assert_eq!(name, "Alice");
                assert_eq!(age, 31, "Alice's age should be updated");
                assert_eq!(city, "Brooklyn", "Alice's city should be updated");
            }
            2 => {
                assert_eq!(name, "Bob");
                assert_eq!(age, 24, "Bob's age should be unchanged");
                assert_eq!(city, "London", "Bob's city should be unchanged");
            }
            3 => {
                assert_eq!(name, "Charlie");
                assert_eq!(age, 35, "Charlie's age should be unchanged");
                assert_eq!(city, "Paris", "Charlie's city should be unchanged");
            }
            4 => {
                assert_eq!(name, "David");
                assert_eq!(age, 29, "David's age should be updated");
                assert_eq!(city, "Brooklyn", "David's city should be updated");
            }
            _ => panic!("Unexpected id: {}", id),
        }
    }
    Ok(())
}

/// Test to verify basic DELETE statement execution
#[test]
fn test_delete_rows_with_engine() -> Result<()> {
    let temp_dir_main = tempdir()?;
    let db_path = temp_dir_main.path().join("test_delete_engine.db");

    let temp_dir_log = tempdir()?;
    let log_path = temp_dir_log.path();
    let log_manager_config = LogManagerConfig {
        log_dir: PathBuf::from(log_path),
        log_file_base_name: "test_wal_delete".to_string(),
        max_log_file_size: 1024 * 1024,
        buffer_config: LogBufferConfig::default(),
        force_sync: false,
    };
    let log_manager = Arc::new(LogManager::new(log_manager_config)?);

    let buffer_pool = Arc::new(BufferPoolManager::new_with_wal(
        100,
        db_path.to_str().unwrap(),
        log_manager.clone(),
    )?);

    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));
    let transaction_manager = Arc::new(TransactionManager::new(log_manager.clone()));

    let engine = ExecutionEngine::new(buffer_pool.clone(), catalog_arc.clone(), transaction_manager.clone());

    let table_name = format!("test_delete_table_{}", rand::random::<u32>());

    let create_query = format!(
        "CREATE TABLE {} (id INT PRIMARY KEY, name TEXT, category TEXT);",
        table_name
    );
    engine.execute_query(&create_query)?;

    // Insert initial data
    let initial_data = vec![
        (1, "Apple", "Fruit"),
        (2, "Banana", "Fruit"),
        (3, "Carrot", "Vegetable"),
        (4, "Broccoli", "Vegetable"),
        (5, "Orange", "Fruit"),
    ];

    for (id, name, category) in initial_data {
        let insert_query = format!(
            "INSERT INTO {} (id, name, category) VALUES ({}, '{}', '{}');",
            table_name, id, name, category
        );
        engine.execute_query(&insert_query)?;
    }

    // Delete rows where category is "Vegetable"
    let delete_query = format!(
        "DELETE FROM {} WHERE category = 'Vegetable';",
        table_name
    );
    let delete_result = engine.execute_query(&delete_query)?;
    assert!(delete_result.rows().first().unwrap().get("status").unwrap().to_string().contains("DELETE"));
    // TODO: Assert number of rows affected if the engine provides this info.

    // Verify the remaining data
    let select_query = format!("SELECT id, name, category FROM {};", table_name);
    let result_set = engine.execute_query(&select_query)?;
    let mut remaining_rows: Vec<Row> = result_set.rows().to_vec();
    remaining_rows.sort_by_key(|row| match row.get("id") { Some(DataValue::Integer(val)) => *val, _ => 0 });

    assert_eq!(remaining_rows.len(), 3, "Should have 3 rows after deleting Vegetables");

    let expected_ids_after_delete = vec![1, 2, 5];
    let actual_ids: Vec<i64> = remaining_rows.iter().map(|row| 
        match row.get("id") { Some(DataValue::Integer(val)) => *val, _ => panic!("Missing id") }
    ).collect();
    assert_eq!(actual_ids, expected_ids_after_delete, "IDs of remaining rows mismatch");

    for row in remaining_rows {
        let category = match row.get("category") { Some(DataValue::Text(val)) => val.clone(), _ => panic!("Missing category") };
        assert_eq!(category, "Fruit", "Only Fruits should remain");
    }

    // Test deleting all remaining rows
    let delete_all_query = format!("DELETE FROM {};", table_name);
    let delete_all_result = engine.execute_query(&delete_all_query)?;
    assert!(delete_all_result.rows().first().unwrap().get("status").unwrap().to_string().contains("DELETE"));

    let select_after_delete_all_query = format!("SELECT id FROM {};", table_name);
    let result_set_after_delete_all = engine.execute_query(&select_after_delete_all_query)?;
    assert_eq!(result_set_after_delete_all.rows().len(), 0, "Table should be empty after deleting all rows");

    Ok(())
} 