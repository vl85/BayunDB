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
use rand;

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
    let log_dir = temp_dir.path().join("logs");
    std::fs::create_dir_all(&log_dir)?;
    
    let buffer_pool = Arc::new(BufferPoolManager::new(
        100,
        db_path.to_str().unwrap()
    )?);
    
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));
    let _engine = ExecutionEngine::new(buffer_pool.clone(), catalog_arc);
    
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

/// Test to verify scanning all rows
#[test]
fn test_scan_all_rows() -> Result<()> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_db_scan_all.db");
    let buffer_pool = Arc::new(BufferPoolManager::new(100, db_path.to_str().unwrap())?);
    let page_manager = PageManager::new();
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));

    let table_name = format!("test_table_scan_all_{}", rand::random::<u32>());

    create_test_table_schema(&catalog_arc, &table_name)?;
    let test_data = generate_data_for_generic_table(19);
    insert_test_data(&table_name, test_data, &buffer_pool, &page_manager, &catalog_arc)?;

    let scan_op = create_table_scan(&table_name, &table_name, buffer_pool.clone(), catalog_arc.clone())
        .map_err(|e| anyhow!(format!("Failed to create scan operator for table {}: {:?}", table_name, e)))?;
    
    let mut op = scan_op.lock().map_err(|e| anyhow!("Failed to lock operator: {}", e))?;
    op.init().map_err(|e| anyhow!("Failed to initialize operator: {:?}", e))?;
    
    let mut found_ids = Vec::new();
    let expected_column_name = format!("{}.id", table_name); // Expect aliased name

    while let Some(row) = op.next()? {
        // println!("[TEST_SCAN_ALL] Row: {:?}", row.columns()); // Debug: print column names in row
        if let Some(data_value) = row.get(&expected_column_name) { // Use aliased name
            if let DataValue::Integer(id_val_ref) = data_value {
                found_ids.push(*id_val_ref);
            } else {
                panic!("Expected INTEGER for column '{}', got {:?}", expected_column_name, data_value);
            }
        } else {
            // If the primary key column is not found with the aliased name, something is wrong
            // Print all available columns for debugging
            let available_columns: Vec<&String> = row.columns().iter().collect();
            panic!(
                "Column '{}' not found in row. Available columns: {:?}. Table name: {}, Alias used: {}", 
                expected_column_name, 
                available_columns,
                table_name, // original table name used for alias
                table_name  // alias itself
            );
        }
    }
    op.close()?;

    let expected_ids: Vec<i64> = (0..19).collect();
    assert_eq!(found_ids, expected_ids, "Expected IDs 0-18 from scan_all_rows");
    Ok(())
} 