// Schema Management Integration Tests
//
// This module provides comprehensive tests for schema management functionality

use std::sync::Arc;
use bayundb::query::executor::engine::ExecutionEngine;
use bayundb::query::parser::parse;
use bayundb::storage::buffer::BufferPoolManager;
use bayundb::catalog::{Catalog, DataType};
use tempfile::NamedTempFile;
use std::sync::RwLock;
use bayundb::transaction::wal::log_manager::{LogManager, LogManagerConfig};
use bayundb::transaction::concurrency::TransactionManager;
use bayundb::transaction::wal::log_buffer::LogBufferConfig;
use std::path::PathBuf;

// Helper function to create an execution engine with a temporary database and a fresh catalog
fn create_test_engine() -> (ExecutionEngine, NamedTempFile, Arc<RwLock<Catalog>>) {
    // Create a temporary database file
    let temp_file = NamedTempFile::new().unwrap();
    let temp_file_path = temp_file.path();
    let db_path_str = temp_file_path.to_str().unwrap().to_string();

    let wal_dir = temp_file_path.parent().map_or_else(
        || PathBuf::from("."), 
        |p| p.to_path_buf()
    );

    let log_file_base_name = temp_file_path
        .file_stem()
        .unwrap_or_default() 
        .to_string_lossy()   
        .into_owned();       
    
    // Create buffer pool manager
    let buffer_pool = Arc::new(BufferPoolManager::new(100, db_path_str).unwrap());
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));

    // Create LogManagerConfig
    let log_manager_config = LogManagerConfig {
        log_dir: wal_dir,
        log_file_base_name,
        max_log_file_size: 10 * 1024 * 1024, // 10MB
        buffer_config: LogBufferConfig::default(), // Default LogBufferConfig
        force_sync: false, // false for tests is usually fine
    };
    
    // Create LogManager
    let log_manager = Arc::new(LogManager::new(log_manager_config).unwrap());

    // Create TransactionManager
    let transaction_manager = Arc::new(TransactionManager::new(log_manager.clone()));
    
    // Create execution engine
    let engine = ExecutionEngine::new(buffer_pool, catalog_arc.clone(), transaction_manager);
    
    (engine, temp_file, catalog_arc)
}

#[test]
fn test_create_table_with_primary_key() {
    // Set up test environment
    let (engine, _temp_file, catalog_arc) = create_test_engine();
    
    // Define a CREATE TABLE statement with a primary key
    let sql = "CREATE TABLE products (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        price FLOAT,
        in_stock BOOLEAN
    )";
    
    // Parse and execute the statement
    let statement = parse(sql).unwrap();
    let result = engine.execute(statement).unwrap();
    
    // Verify execution result
    assert_eq!(result.row_count(), 1);
    
    // Access the catalog for this test
    let catalog = catalog_arc.read().unwrap();
    
    // Check that the catalog has the table
    assert!(catalog.table_exists("products"));
    
    // Get the table and verify its structure
    let table = catalog.get_table("products").unwrap();
    assert_eq!(table.name(), "products");
    assert_eq!(table.columns().len(), 4);
    
    // Check primary key
    let id_col = table.get_column("id").unwrap();
    assert_eq!(id_col.is_primary_key(), true);
    assert_eq!(id_col.is_nullable(), false); // Primary keys can't be NULL
    
    // Verify NOT NULL constraint is respected
    let name_col = table.get_column("name").unwrap();
    assert_eq!(name_col.is_nullable(), false);
    
    // Verify default nullability for other columns
    let price_col = table.get_column("price").unwrap();
    assert_eq!(price_col.is_nullable(), true);
}

#[test]
fn test_create_tables_with_different_data_types() {
    // Set up test environment
    let (engine, _temp_file, catalog_arc) = create_test_engine();
    
    // Define a CREATE TABLE statement with various data types
    let sql = "CREATE TABLE data_types_test (
        int_col INTEGER,
        float_col FLOAT,
        text_col TEXT,
        bool_col BOOLEAN,
        date_col DATE,
        timestamp_col TIMESTAMP
    )";
    
    // Parse and execute the statement
    let statement = parse(sql).unwrap();
    let result = engine.execute(statement).unwrap();
    
    // Verify execution result
    assert_eq!(result.row_count(), 1);
    
    // Access the catalog for this test
    let catalog = catalog_arc.read().unwrap();
    
    // Check that the catalog has the table
    assert!(catalog.table_exists("data_types_test"));
    
    // Get the table and verify data types
    let table = catalog.get_table("data_types_test").unwrap();
    
    assert_eq!(*table.get_column("int_col").unwrap().data_type(), DataType::Integer);
    assert_eq!(*table.get_column("float_col").unwrap().data_type(), DataType::Float);
    assert_eq!(*table.get_column("text_col").unwrap().data_type(), DataType::Text);
    assert_eq!(*table.get_column("bool_col").unwrap().data_type(), DataType::Boolean);
    assert_eq!(*table.get_column("date_col").unwrap().data_type(), DataType::Date);
    assert_eq!(*table.get_column("timestamp_col").unwrap().data_type(), DataType::Timestamp);
}

#[test]
fn test_multiple_table_creation() {
    // Set up test environment
    let (engine, _temp_file, catalog_arc) = create_test_engine();
    
    // Create first table
    let sql1 = "CREATE TABLE customers (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT
    )";
    
    // Create second table
    let sql2 = "CREATE TABLE orders (
        id INTEGER PRIMARY KEY,
        customer_id INTEGER NOT NULL,
        amount FLOAT,
        order_date TIMESTAMP
    )";
    
    // Parse and execute statements
    let statement1 = parse(sql1).unwrap();
    let statement2 = parse(sql2).unwrap();
    
    engine.execute(statement1).unwrap();
    engine.execute(statement2).unwrap();
    
    // Access the catalog for this test
    let catalog = catalog_arc.read().unwrap();
    
    // Check that both tables exist
    assert!(catalog.table_exists("customers"));
    assert!(catalog.table_exists("orders"));
    
    // Verify table structures
    let customers = catalog.get_table("customers").unwrap();
    let orders = catalog.get_table("orders").unwrap();
    
    assert_eq!(customers.columns().len(), 3);
    assert_eq!(orders.columns().len(), 4);
}

// Error cases could be added in a real implementation, for example:
// - Creating a table that already exists
// - Invalid data types
// - Missing required constraints
// - Other schema validation errors 