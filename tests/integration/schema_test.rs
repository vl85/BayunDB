// Schema Management Integration Tests
//
// This module tests the CREATE TABLE functionality

use std::sync::Arc;
use bayundb::query::executor::engine::ExecutionEngine;
use bayundb::query::parser::parse;
use bayundb::storage::buffer::BufferPoolManager;
use bayundb::catalog::{Catalog, DataType};
use tempfile::NamedTempFile;

#[test]
fn test_create_table() {
    // Create a temporary database file
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    
    // Create buffer pool manager
    let buffer_pool = Arc::new(BufferPoolManager::new(100, path).unwrap());
    
    // Create execution engine
    let engine = ExecutionEngine::new(buffer_pool);
    
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
    
    // Access the global catalog
    let catalog_instance = Catalog::instance();
    let catalog = catalog_instance.read().unwrap();
    
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
    
    // Create execution engine
    let engine = ExecutionEngine::new(buffer_pool);
    
    // Define a simpler CREATE TABLE statement
    let sql = "CREATE TABLE users (id INTEGER, name TEXT)";
    
    // Parse and execute the statement
    let statement = parse(sql).unwrap();
    let result = engine.execute(statement).unwrap();
    
    // Just check that execution succeeded
    assert_eq!(result.row_count(), 1);
    
    // Verify catalog contains the table
    let catalog_instance = Catalog::instance();
    let catalog = catalog_instance.read().unwrap();
    
    assert!(catalog.table_exists("users"));
    
    let table = catalog.get_table("users").unwrap();
    assert_eq!(table.name(), "users");
    assert_eq!(table.columns().len(), 2);
} 