// Schema Management Integration Tests
//
// This module tests the CREATE TABLE functionality

use anyhow::Result;
use bayundb::catalog::{Catalog, DataType};
use bayundb::query::executor::engine::ExecutionEngine;
use bayundb::query::executor::operators::scan::TableScanOperator;
use bayundb::query::executor::operators::Operator;
use bayundb::query::executor::result::DataValue;
use bayundb::query::parser::parse;
use bayundb::storage::buffer::BufferPoolManager;
use bayundb::storage::page::PageManager;
use std::sync::Arc;
use tempfile::NamedTempFile;
use std::sync::RwLock;

// Declare the common module for test utilities using a path attribute
#[path = "../common/mod.rs"]
mod common;
use common::insert_test_data;

#[test]
fn test_create_table() {
    // Create a temporary database file
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    
    // Create buffer pool manager
    let buffer_pool = Arc::new(BufferPoolManager::new(100, path).unwrap());
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));
    
    // Create execution engine
    let engine = ExecutionEngine::new(buffer_pool.clone(), catalog_arc.clone());
    
    // Define a CREATE TABLE statement
    let sql = "CREATE TABLE employees (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        department TEXT,
        salary FLOAT,
        hire_date DATE,
        active BOOLEAN
    )";
    
    // Parse and execute the statement
    let statement = parse(sql).unwrap();
    let _result = engine.execute(statement).unwrap();
    
    // Access the catalog created for this test
    let catalog = catalog_arc.read().unwrap();
    
    // Check that the catalog now has the table
    assert!(catalog.table_exists("employees"));
    
    // Get the table and verify its structure
    if let Some(table) = catalog.get_table("employees") {
        assert_eq!(table.name(), "employees");
        assert_eq!(table.columns().len(), 6);
        
        // Check id column
        let id_col = table.get_column("id").unwrap();
        assert_eq!(id_col.name(), "id");
        assert_eq!(*id_col.data_type(), DataType::Integer);
        assert_eq!(id_col.is_primary_key(), true);
        assert_eq!(id_col.is_nullable(), false);
        
        // Check name column
        let name_col = table.get_column("name").unwrap();
        assert_eq!(name_col.name(), "name");
        assert_eq!(*name_col.data_type(), DataType::Text);
        assert_eq!(name_col.is_nullable(), false);
        
        // Check salary column
        let salary_col = table.get_column("salary").unwrap();
        assert_eq!(salary_col.name(), "salary");
        assert_eq!(*salary_col.data_type(), DataType::Float);
        assert_eq!(salary_col.is_nullable(), true);
    } else {
        panic!("Table 'employees' not found in catalog");
    }
}

#[test]
fn test_simple_create_table() {
    // Create a temporary database file
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    
    // Create buffer pool manager
    let buffer_pool = Arc::new(BufferPoolManager::new(100, path).unwrap());
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));
    
    // Create execution engine
    let engine = ExecutionEngine::new(buffer_pool.clone(), catalog_arc.clone());
    
    // Define a simpler CREATE TABLE statement
    let sql = "CREATE TABLE users (id INTEGER, name TEXT)";
    
    // Parse and execute the statement
    let statement = parse(sql).unwrap();
    let result = engine.execute(statement).unwrap();
    
    // Just check that execution succeeded
    assert_eq!(result.row_count(), 1);
    
    // Verify catalog contains the table
    let catalog = catalog_arc.read().unwrap();
    
    assert!(catalog.table_exists("users"));
    
    let table = catalog.get_table("users").unwrap();
    assert_eq!(table.name(), "users");
    assert_eq!(table.columns().len(), 2);
}

#[test]
fn test_create_table_with_execute_query() {
    // Create a temporary database file
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    
    // Create buffer pool manager
    let buffer_pool = Arc::new(BufferPoolManager::new(100, path).unwrap());
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));
    
    // Create execution engine
    let engine = ExecutionEngine::new(buffer_pool.clone(), catalog_arc.clone());
    
    // Use execute_query directly with SQL (this would have hung before our fix)
    let sql = "CREATE TABLE products (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        price FLOAT,
        stock INTEGER
    )";
    
    // Execute the query string directly
    let result = engine.execute_query(sql).unwrap();
    
    // Print the actual row count for debugging
    println!("Actual result.row_count(): {}", result.row_count());
    
    // Verify execution succeeded - the result gives 1 row with the success message
    assert_eq!(result.row_count(), 1);
    
    // Verify the table was created in the catalog
    let catalog = catalog_arc.read().unwrap();
    
    assert!(catalog.table_exists("products"));
    
    // Test that we can query the table after creating it
    let query_result = engine.execute_query("SELECT * FROM products").unwrap();
    // Print the actual row count
    println!("SELECT result row count: {}", query_result.row_count());
    
    // Just check that we have zero rows in the result for a newly created empty table
    assert_eq!(query_result.row_count(), 0, "Should have zero rows in a newly created, empty products table");
    
    // Print the first few rows for debugging
    let rows = query_result.rows();
    for (i, row) in rows.iter().take(5).enumerate() {
        println!("Row {}: {:?}", i, row);
    }
}

#[test]
fn test_scan_operator_uses_catalog() {
    // Create a temporary database file
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    
    // Create buffer pool manager
    let buffer_pool = Arc::new(BufferPoolManager::new(100, path).unwrap());
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));
    
    // Create execution engine
    let engine = ExecutionEngine::new(buffer_pool.clone(), catalog_arc.clone());
    
    // Define a CREATE TABLE statement
    let sql = "CREATE TABLE dynamic_table (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        active BOOLEAN,
        score FLOAT
    )";
    
    // Execute the query string directly
    let result = engine.execute_query(sql).unwrap();
    
    // Verify execution succeeded
    assert_eq!(result.row_count(), 1);
    
    // Verify the table was created in the catalog
    let catalog_read_guard = catalog_arc.read().unwrap();
    
    assert!(catalog_read_guard.table_exists("dynamic_table"));
    drop(catalog_read_guard);
    
    // Now create a TableScanOperator directly to test it uses catalog data
    use bayundb::query::executor::operators::scan::TableScanOperator;
    use bayundb::query::executor::operators::Operator;
    use bayundb::storage::page::PageManager;
    
    let page_manager = PageManager::new();
    let mut scan_op = TableScanOperator::new(
        "dynamic_table".to_string(), 
        "dt".to_string(), 
        buffer_pool.clone(),
        page_manager,
        catalog_arc.clone()
    );
    
    // Initialize the scan
    scan_op.init().unwrap();
    
    // Retrieve a few rows
    let mut rows = Vec::new();
    for _ in 0..3 { // This loop will not add anything if the table is empty
        if let Some(row) = scan_op.next().unwrap() {
            rows.push(row);
        }
    }
    
    // Verify we got no rows from an empty table
    assert!(rows.is_empty(), "Should not have found any rows in an empty table");
    
    // Close the scan
    scan_op.close().unwrap();
}

#[test]
fn test_scan_operator_with_different_column_types() -> Result<()> {
    // Create a temporary database file
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    
    let buffer_pool = Arc::new(BufferPoolManager::new(100, &path)?);
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));
    let engine = ExecutionEngine::new(buffer_pool.clone(), catalog_arc.clone());
    
    let sql = "CREATE TABLE all_types_table (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        active BOOLEAN,
        score FLOAT,
        created_date DATE,
        updated_timestamp TIMESTAMP
    )";
    
    let create_result = engine.execute_query(sql)?;
    assert_eq!(create_result.row_count(), 1);

    let catalog_read_guard = catalog_arc.read().unwrap();
    assert!(catalog_read_guard.table_exists("all_types_table"));
    let table_schema = catalog_read_guard.get_table("all_types_table").unwrap();
    assert_eq!(table_schema.columns().len(), 6, "Table should have 6 columns");
    drop(catalog_read_guard);

    let data_to_insert: Vec<Vec<DataValue>> = vec![
        vec![
            DataValue::Integer(1),
            DataValue::Text("Alice".to_string()),
            DataValue::Boolean(true),
            DataValue::Float(95.5),
            DataValue::Text("2023-01-15".to_string()), 
            DataValue::Text("2023-01-15T10:30:00".to_string())
        ],
        vec![
            DataValue::Integer(2),
            DataValue::Text("Bob".to_string()),
            DataValue::Boolean(false),
            DataValue::Float(88.0),
            DataValue::Text("2023-02-20".to_string()),
            DataValue::Text("2023-02-20T12:00:00".to_string())
        ],
        // Add one more row for variety, ensuring it matches schema
        vec![
            DataValue::Integer(3),
            DataValue::Text("Charlie".to_string()),
            DataValue::Boolean(true),
            DataValue::Float(77.7),
            DataValue::Text("2023-03-25".to_string()), 
            DataValue::Text("2023-03-25T15:00:00".to_string())
        ],
    ];

    let page_manager_for_insert = PageManager::new();
    insert_test_data("all_types_table", data_to_insert.clone(), &buffer_pool, &page_manager_for_insert, &catalog_arc)?;

    let page_manager_for_scan = PageManager::new();
    let mut scan_op = TableScanOperator::new(
        "all_types_table".to_string(), 
        "t".to_string(), 
        buffer_pool.clone(),
        page_manager_for_scan, 
        catalog_arc.clone()
    );
    
    scan_op.init().unwrap();
    
    let mut retrieved_rows = Vec::new();
    let mut count = 0;
    while let Some(row_result) = scan_op.next()? {
        retrieved_rows.push(row_result);
        count += 1;
        if count >= 5 { // Limit to 5 as in original test logic, or match data_to_insert.len()
            break;
        }
    }
    
    assert!(!retrieved_rows.is_empty(), "Should have retrieved test rows after insertion");
    // The original test tried to fetch 5 rows, we inserted 3. So, we expect 3.
    assert_eq!(retrieved_rows.len(), data_to_insert.len(), "Expected to retrieve all inserted rows");

    for (idx, row) in retrieved_rows.iter().enumerate() {
        let original_row = &data_to_insert[idx];
        println!("Comparing retrieved row: {:?} with original: {:?}", row.values_with_names().collect::<Vec<_>>(), original_row);

        let id = row.get("t.id").expect("Should have id column");
        assert_eq!(id, &original_row[0], "Mismatch in id column");
        assert!(matches!(id, DataValue::Integer(_)), "id should be an Integer");
        
        let name = row.get("t.name").expect("Should have name column");
        assert_eq!(name, &original_row[1], "Mismatch in name column");
        assert!(matches!(name, DataValue::Text(_)), "name should be Text");
        
        let active = row.get("t.active").expect("Should have active column");
        assert_eq!(active, &original_row[2], "Mismatch in active column");
        assert!(matches!(active, DataValue::Boolean(_)), "active should be Boolean");
        
        let score = row.get("t.score").expect("Should have score column");
        assert_eq!(score, &original_row[3], "Mismatch in score column");
        assert!(matches!(score, DataValue::Float(_)), "score should be Float");
        
        let date = row.get("t.created_date").expect("Should have created_date column");
        assert_eq!(date, &original_row[4], "Mismatch in created_date column");
        assert!(matches!(date, DataValue::Text(_)), "created_date should be Text");
        if let DataValue::Text(date_str) = date {
            assert!(date_str.contains("-"), "Date should contain hyphens");
        }
        
        let timestamp = row.get("t.updated_timestamp").expect("Should have updated_timestamp column");
        assert_eq!(timestamp, &original_row[5], "Mismatch in updated_timestamp column");
        assert!(matches!(timestamp, DataValue::Text(_)), "updated_timestamp should be Text");
        if let DataValue::Text(ts_str) = timestamp {
            assert!(ts_str.contains(":"), "Timestamp should contain colons");
        }
    }
    
    scan_op.close().unwrap();
    Ok(())
}

#[test]
fn test_scan_operator_returns_empty_result_for_empty_table() {
    // Create a temporary database file
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    
    // Create buffer pool manager
    let buffer_pool = Arc::new(BufferPoolManager::new(100, path).unwrap());
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));
    
    // Create execution engine
    let engine = ExecutionEngine::new(buffer_pool.clone(), catalog_arc.clone());
    
    // Create a simple table
    let sql = "CREATE TABLE empty_table (id INTEGER PRIMARY KEY, name TEXT)";
    let result = engine.execute_query(sql).unwrap();
    assert_eq!(result.row_count(), 1, "Table creation should succeed");
    
    // Verify the table exists in catalog
    let catalog_read_guard = catalog_arc.read().unwrap();
    assert!(catalog_read_guard.table_exists("empty_table"));
    
    // Test using the scan operator directly
    use bayundb::query::executor::operators::scan::TableScanOperator;
    use bayundb::query::executor::operators::Operator;
    use bayundb::storage::page::PageManager;
    
    let page_manager = PageManager::new();
    let mut scan_op = TableScanOperator::new(
        "empty_table".to_string(), 
        "t".to_string(), 
        buffer_pool.clone(),
        page_manager,
        catalog_arc.clone()
    );
    scan_op.init().unwrap();
    
    // The scan should not return any rows for an empty table
    let row = scan_op.next().unwrap();
    assert!(row.is_none(), "Empty table should return no rows");
    
    println!("Scan operator correctly returns no rows for empty table");
    
    // Also test with execute_query
    let query_result = engine.execute_query("SELECT * FROM empty_table").unwrap();
    println!("SELECT returned {} rows from empty table", query_result.row_count());
    assert_eq!(query_result.row_count(), 0, "Empty table should have 0 rows");
    
    // Close the scan
    scan_op.close().unwrap();
}

#[test]
fn test_catalog_empty_table_direct_check() {
    // Create a temporary database file
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    
    // Create buffer pool manager
    let buffer_pool = Arc::new(BufferPoolManager::new(100, path).unwrap());
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));
    
    // Create execution engine
    let engine = ExecutionEngine::new(buffer_pool.clone(), catalog_arc.clone());
    
    // Create a simple empty table
    let sql = "CREATE TABLE empty_catalog_table (id INTEGER PRIMARY KEY, value TEXT)";
    let result = engine.execute_query(sql).unwrap();
    assert_eq!(result.row_count(), 1, "Table creation should succeed");
    
    // Access the catalog directly
    let catalog_read_guard = catalog_arc.read().unwrap();
    
    // Verify table exists in catalog
    assert!(catalog_read_guard.table_exists("empty_catalog_table"), "Table should exist in catalog");
    
    // Get the table and check its properties
    if let Some(table) = catalog_read_guard.get_table("empty_catalog_table") {
        // Verify table name
        assert_eq!(table.name(), "empty_catalog_table", "Table name should match");
        
        // Verify column count and properties
        assert_eq!(table.columns().len(), 2, "Table should have 2 columns");
        
        // Check id column
        let id_col = table.get_column("id").unwrap();
        assert_eq!(id_col.name(), "id");
        assert_eq!(*id_col.data_type(), DataType::Integer);
        assert_eq!(id_col.is_primary_key(), true);
        
        // Check value column
        let value_col = table.get_column("value").unwrap();
        assert_eq!(value_col.name(), "value");
        assert_eq!(*value_col.data_type(), DataType::Text);
        
        // Verify first_page_id is None (indicating empty table)
        assert_eq!(table.first_page_id(), None, "Empty table should have no first page ID");
        
        println!("Empty table correctly exists in catalog with no pages");
    } else {
        panic!("Failed to get table from catalog");
    }
} 