// Debug example for ExecutionEngine test issues
//
// This example demonstrates how to properly test the ExecutionEngine
// without running into timeout or errors. This serves as a template for 
// fixing the test_execution_engine test in the engine module.

use std::sync::Arc;
use bayundb::query::executor::engine::ExecutionEngine;
use bayundb::query::parser::ast::{
    Statement, SelectStatement, TableReference, SelectColumn, ColumnReference
};
use bayundb::query::planner::Planner;
use bayundb::query::planner::PhysicalPlan;
use bayundb::storage::buffer::BufferPoolManager;
use tempfile::NamedTempFile;
use bayundb::catalog::Catalog;
use std::sync::RwLock;
use bayundb::transaction::concurrency::transaction_manager::TransactionManager;
use bayundb::transaction::wal::log_manager::{LogManager, LogManagerConfig};
use bayundb::transaction::wal::log_buffer::LogBufferConfig;
use std::path::PathBuf;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("ExecutionEngine Debug Example");
    println!("============================");
    
    let temp_db_file = NamedTempFile::new()?;
    let db_path = temp_db_file.path().to_str().unwrap().to_string();
    println!("Created temporary database at: {}", db_path);

    let temp_log_dir = tempdir()?;
    let log_path = temp_log_dir.path();
    println!("Created temporary log directory at: {:?}", log_path);

    let log_manager_config = LogManagerConfig {
        log_dir: PathBuf::from(log_path),
        log_file_base_name: "example_wal".to_string(),
        max_log_file_size: 1024 * 1024,
        buffer_config: LogBufferConfig::default(),
        force_sync: true,
    };
    let log_manager = Arc::new(LogManager::new(log_manager_config)?);
    println!("Log manager initialized");
    
    let buffer_pool = Arc::new(BufferPoolManager::new_with_wal(10, db_path, log_manager.clone())?);
    println!("Buffer pool initialized with WAL");
    
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));
    let transaction_manager = Arc::new(TransactionManager::new(log_manager.clone()));
    println!("Transaction manager created");

    // Create execution engine
    let engine = ExecutionEngine::new(buffer_pool.clone(), catalog_arc.clone(), transaction_manager.clone());
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
        order_by: Vec::new(),
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
        
        if let Ok(_phys_plan) = physical_plan {
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