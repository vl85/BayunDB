use bayundb::query::executor::engine::ExecutionEngine;
use bayundb::storage::buffer::BufferPoolManager;
use bayundb::catalog::Catalog;
use bayundb::transaction::concurrency::transaction_manager::TransactionManager;
use bayundb::transaction::wal::log_manager::{LogManager, LogManagerConfig};
use bayundb::transaction::wal::log_buffer::LogBufferConfig;
use std::sync::{Arc, RwLock};
use tempfile::{NamedTempFile, TempDir};
use std::path::PathBuf;

fn main() {
    println!("--- Starting debug_alter_insert_bug example ---");

    // Setup temporary database file and WAL directory
    let temp_db_file = NamedTempFile::new().expect("Failed to create temp db file");
    let db_path = temp_db_file.path().to_str().unwrap().to_string();
    
    let temp_log_dir = TempDir::new().expect("Failed to create temp log dir");
    let log_config = LogManagerConfig {
        log_dir: PathBuf::from(temp_log_dir.path()),
        log_file_base_name: "debug_wal".to_string(),
        max_log_file_size: 1024 * 10, // 10KB
        buffer_config: LogBufferConfig::default(),
        force_sync: false,
    };
    let log_manager = Arc::new(LogManager::new(log_config).expect("Failed to create LogManager"));
    
    // Initialize components
    let buffer_pool = Arc::new(
        BufferPoolManager::new_with_wal(100, db_path.clone(), log_manager.clone())
            .expect("Failed to create BufferPoolManager")
    );
    let catalog = Arc::new(RwLock::new(Catalog::new()));
    let transaction_manager = Arc::new(TransactionManager::new(log_manager.clone()));

    let engine = ExecutionEngine::new(buffer_pool.clone(), catalog.clone(), transaction_manager.clone());

    let queries = vec![
        "CREATE TABLE test_alter_bug (id INT);",
        "INSERT INTO test_alter_bug (id) VALUES (1);",
        "ALTER TABLE test_alter_bug ADD COLUMN data TEXT;",
        "INSERT INTO test_alter_bug (id, data) VALUES (2, 'row2');",
        "SELECT * FROM test_alter_bug;",
    ];

    for (i, query_str) in queries.iter().enumerate() {
        println!("\\n--- Executing Query {}: {} ---", i + 1, query_str);
        match engine.execute_query(query_str) {
            Ok(result_set) => {
                if query_str.trim().to_lowercase().starts_with("select") {
                    println!("Query Result ({} rows):", result_set.row_count());
                    if result_set.row_count() > 0 {
                        println!("Columns: {:?}", result_set.columns());
                        for row in result_set.rows() {
                           let row_values: Vec<_> = result_set.columns().iter().map(|col_name| {
                                format!("{}: {:?}", col_name, row.get(col_name).unwrap_or(&bayundb::query::executor::result::DataValue::Null))
                            }).collect();
                            println!("  Row: {{{}}}", row_values.join(", "));
                        }
                    }
                } else {
                    println!("Query executed successfully.");
                    if result_set.row_count() > 0 {
                         for row in result_set.rows() {
                           let row_values: Vec<_> = result_set.columns().iter().map(|col_name| {
                                format!("{}: {:?}", col_name, row.get(col_name).unwrap_or(&bayundb::query::executor::result::DataValue::Null))
                            }).collect();
                            println!("  Status: {{{}}}", row_values.join(", "));
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Error executing query '{}': {:?}", query_str, e);
            }
        }
    }

    println!("\\n--- debug_alter_insert_bug example finished ---");
    // temp_db_file and temp_log_dir will be cleaned up when they go out of scope.
} 