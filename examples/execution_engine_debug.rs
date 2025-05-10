// Debug example for ExecutionEngine test issues
//
// This example demonstrates how to properly test the ExecutionEngine
// without running into timeout or errors. This serves as a template for 
// fixing the test_execution_engine test in the engine module.

use std::sync::Arc;
use bayundb::query::executor::engine::ExecutionEngine;
use bayundb::query::executor::result::QueryResult;
use bayundb::query::parser::ast::{
    Statement, SelectStatement, TableReference, SelectColumn, ColumnReference
};
use bayundb::query::planner::Planner;
use bayundb::query::planner::PhysicalPlan;
use bayundb::storage::buffer::BufferPoolManager;
use tempfile::NamedTempFile;
use bayundb::catalog::Catalog;
use std::sync::RwLock;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("ExecutionEngine Debug Example");
    println!("============================");
    
    // Create a temporary test database
    let temp_file = NamedTempFile::new()?;
    let db_path = temp_file.path().to_str().unwrap().to_string();
    println!("Created temporary database at: {}", db_path);
    
    // Create buffer pool manager with small size
    let buffer_pool = Arc::new(BufferPoolManager::new(10, db_path)?);
    println!("Buffer pool initialized");
    
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));

    // Create execution engine
    let engine = ExecutionEngine::new(buffer_pool.clone(), catalog_arc.clone());
    println!("Execution engine created");
    
    // Correct approach to testing: Create a SELECT statement with test_table
    // Using test_table is important because this is what the mock scan operator supports
    let select = Statement::Select(SelectStatement {
        columns: vec![
            SelectColumn::Column(ColumnReference {
                table: None,
                name: "id".to_string()
            })
        ],
        from: vec![TableReference {
            name: "test_table".to_string(), // Using test_table which mock handlers support
            alias: None,
        }],
        where_clause: None,
        joins: vec![],
        group_by: None,
        having: None,
    });
    println!("SELECT statement created");
    
    // CORRECT METHOD #1: Use the public execute method instead of directly accessing planner
    println!("\nTesting proper execution approach:");
    let result = engine.execute(select.clone());
    match result {
        Ok(result_set) => println!("Execution succeeded with {} rows", result_set.row_count()),
        Err(e) => println!("Execution failed (expected): {:?}", e),
    }
    
    // CORRECT METHOD #2: Create a separate planner for testing cost estimation
    // This allows testing similar functionality without relying on private fields
    println!("\nTesting with separate planner for plan creation and cost estimation:");
    let standalone_planner = Planner::new(buffer_pool.clone(), catalog_arc.clone());
    
    // Create logical plan
    let logical_plan = standalone_planner.create_logical_plan(&select);
    println!("Logical plan created: {:?}", logical_plan.is_ok());
    
    if let Ok(plan) = logical_plan {
        let physical_plan = standalone_planner.create_physical_plan(&plan);
        println!("Physical plan created: {:?}", physical_plan.is_ok());
        
        if let Ok(phys_plan) = physical_plan {
            // Test cost estimation
            let cost = standalone_planner.estimate_cost(&PhysicalPlan::SeqScan {
                table_name: "test_table".to_string(),
                alias: None,
            });
            println!("Cost of SeqScan: {}", cost);
        }
    }
    
    // CORRECT METHOD #3: Use execute_query for string-based queries
    println!("\nTesting query execution on test_table:");
    let result = engine.execute_query("SELECT * FROM test_table");
    match result {
        Ok(result_set) => {
            println!("Query succeeded with {} rows", result_set.row_count());
            println!("Columns: {:?}", result_set.columns());
            
            // Print first few rows
            println!("\nFirst rows:");
            for (i, row) in result_set.rows().iter().take(3).enumerate() {
                println!("Row {}: {:?}", i, row);
            }
        },
        Err(e) => println!("Query failed: {:?}", e),
    }
    
    println!("\nExecution engine debug complete");
    Ok(())
} 