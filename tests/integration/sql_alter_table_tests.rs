// tests/integration/sql_alter_table_tests.rs

use bayundb::query::executor::ExecutionEngine;
use bayundb::storage::buffer::BufferPoolManager;
use bayundb::catalog::Catalog;
use bayundb::transaction::concurrency::transaction_manager::TransactionManager;
use bayundb::transaction::wal::log_manager::{LogManager, LogManagerConfig};
use bayundb::transaction::wal::log_buffer::LogBufferConfig;
use std::sync::Arc;
use tempfile::TempDir;
use anyhow::Result;

// Helper function to set up a test database instance and execution engine
fn setup_test_db() -> Result<(ExecutionEngine, TempDir)> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_alter.db");
    let log_dir_path = temp_dir.path().join("test_alter_logs");
    std::fs::create_dir_all(&log_dir_path)?;

    let log_config = LogManagerConfig {
        log_dir: log_dir_path,
        log_file_base_name: "bayun_test_log".to_string(),
        max_log_file_size: 1024 * 1024, // 1 MB
        buffer_config: LogBufferConfig::default(),
        force_sync: false, // Typically false for tests unless testing durability specifically
    };
    let log_manager = Arc::new(LogManager::new(log_config)?);
    
    let buffer_pool = Arc::new(BufferPoolManager::new_with_wal(
        100, // Small buffer pool for tests
        &db_path,
        log_manager.clone(),
    )?);
    
    let catalog = Catalog::instance(); // Use the global catalog for now, or a fresh one if needed
                                     // For truly isolated tests, a new Catalog::new() might be better.
                                     // Let's start with the global one and refine if tests interfere.
    
    let transaction_manager = Arc::new(TransactionManager::new(log_manager.clone()));
    
    let engine = ExecutionEngine::new(buffer_pool.clone(), catalog, transaction_manager.clone());

    Ok((engine, temp_dir))
}

fn execute_query_assert_ok(engine: &ExecutionEngine, query: &str) -> bayundb::query::executor::result::QueryResultSet {
    println!("Executing: {}", query);
    let result = engine.execute_query(query);
    if let Err(e) = &result {
        eprintln!("Error executing query \"{}\": {:?}", query, e); // Log to stderr for test output visibility
    }
    result.expect("Query execution failed") // Use expect to panic on error, showing the query
}

#[cfg(test)]
mod alter_table_add_column_tests {
    use super::*;
    use bayundb::query::executor::result::DataValue;

    #[test]
    fn test_add_column_basic_no_default() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;

        // 1. Create initial table and insert data
        execute_query_assert_ok(&engine, "CREATE TABLE test_add_basic (id INT, name TEXT);");
        execute_query_assert_ok(&engine, "INSERT INTO test_add_basic VALUES (1, 'Alice');");
        execute_query_assert_ok(&engine, "INSERT INTO test_add_basic VALUES (2, 'Bob');");

        // 2. Add the new column
        execute_query_assert_ok(&engine, "ALTER TABLE test_add_basic ADD COLUMN city TEXT;");

        // 3. Verifications
        let result = execute_query_assert_ok(&engine, "SELECT id, name, city FROM test_add_basic ORDER BY id;");
        assert_eq!(result.columns(), vec!["id", "name", "city"]);
        let rows = result.rows();
        assert_eq!(rows.len(), 2);
        
        // Row 1
        assert_eq!(rows[0].get("id"), Some(&DataValue::Integer(1)));
        assert_eq!(rows[0].get("name"), Some(&DataValue::Text("Alice".to_string())));
        assert_eq!(rows[0].get("city"), Some(&DataValue::Null)); // Default NULL

        // Row 2
        assert_eq!(rows[1].get("id"), Some(&DataValue::Integer(2)));
        assert_eq!(rows[1].get("name"), Some(&DataValue::Text("Bob".to_string())));
        assert_eq!(rows[1].get("city"), Some(&DataValue::Null)); // Default NULL

        // Try inserting a new row including the new column
        execute_query_assert_ok(&engine, "INSERT INTO test_add_basic (id, name, city) VALUES (3, 'Charlie', 'New York');");
        let result_charlie = execute_query_assert_ok(&engine, "SELECT city FROM test_add_basic WHERE id = 3;");
        assert_eq!(result_charlie.rows()[0].get("city"), Some(&DataValue::Text("New York".to_string())));
        
        // Try inserting a new row omitting the new column (should default to NULL)
        execute_query_assert_ok(&engine, "INSERT INTO test_add_basic (id, name) VALUES (4, 'David');");
        let result_david = execute_query_assert_ok(&engine, "SELECT city FROM test_add_basic WHERE id = 4;");
        assert_eq!(result_david.rows()[0].get("city"), Some(&DataValue::Null));

        Ok(())
    }

    #[test]
    fn test_add_column_with_default() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;

        execute_query_assert_ok(&engine, "CREATE TABLE test_add_default (id INT, value INT);");
        execute_query_assert_ok(&engine, "INSERT INTO test_add_default VALUES (1, 100);");
        execute_query_assert_ok(&engine, "INSERT INTO test_add_default VALUES (2, 200);");

        execute_query_assert_ok(&engine, "ALTER TABLE test_add_default ADD COLUMN category TEXT DEFAULT 'General';");

        // 1. Check existing rows - should have the default value
        let result_existing = execute_query_assert_ok(&engine, "SELECT id, value, category FROM test_add_default ORDER BY id;");
        assert_eq!(result_existing.columns(), vec!["id", "value", "category"]);
        let rows_existing = result_existing.rows();
        assert_eq!(rows_existing.len(), 2);
        assert_eq!(rows_existing[0].get("category"), Some(&DataValue::Text("General".to_string())));
        assert_eq!(rows_existing[1].get("category"), Some(&DataValue::Text("General".to_string())));

        // 2. Insert a new row providing the new column
        execute_query_assert_ok(&engine, "INSERT INTO test_add_default (id, value, category) VALUES (3, 300, 'Specific');");
        let result_new_provided = execute_query_assert_ok(&engine, "SELECT category FROM test_add_default WHERE id = 3;");
        assert_eq!(result_new_provided.rows()[0].get("category"), Some(&DataValue::Text("Specific".to_string())));

        // 3. Insert a new row omitting the new column (should use default)
        execute_query_assert_ok(&engine, "INSERT INTO test_add_default (id, value) VALUES (4, 400);");
        let result_new_defaulted = execute_query_assert_ok(&engine, "SELECT category FROM test_add_default WHERE id = 4;");
        assert_eq!(result_new_defaulted.rows()[0].get("category"), Some(&DataValue::Text("General".to_string())));
        
        // 4. Add another column with a different default type (e.g. INT)
        execute_query_assert_ok(&engine, "ALTER TABLE test_add_default ADD COLUMN quantity INT DEFAULT 0;");
        let result_q_existing = execute_query_assert_ok(&engine, "SELECT quantity FROM test_add_default WHERE id = 1;");
        assert_eq!(result_q_existing.rows()[0].get("quantity"), Some(&DataValue::Integer(0)));
        
        execute_query_assert_ok(&engine, "INSERT INTO test_add_default (id, value, category, quantity) VALUES (5, 500, 'Mixed', 50);");
        let result_q_new = execute_query_assert_ok(&engine, "SELECT quantity FROM test_add_default WHERE id = 5;");
        assert_eq!(result_q_new.rows()[0].get("quantity"), Some(&DataValue::Integer(50)));

        execute_query_assert_ok(&engine, "INSERT INTO test_add_default (id, value, category) VALUES (6, 600, 'NoQuantity');");
        let result_q_defaulted = execute_query_assert_ok(&engine, "SELECT quantity FROM test_add_default WHERE id = 6;");
        assert_eq!(result_q_defaulted.rows()[0].get("quantity"), Some(&DataValue::Integer(0)));

        Ok(())
    }

    #[test]
    fn test_add_column_not_null_with_default() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;

        execute_query_assert_ok(&engine, "CREATE TABLE test_add_not_null_default (id INT);");
        execute_query_assert_ok(&engine, "INSERT INTO test_add_not_null_default VALUES (1);");
        execute_query_assert_ok(&engine, "INSERT INTO test_add_not_null_default VALUES (2);");

        // Add a NOT NULL column with a DEFAULT value. This should succeed, and existing rows get the default.
        execute_query_assert_ok(&engine, "ALTER TABLE test_add_not_null_default ADD COLUMN name TEXT NOT NULL DEFAULT 'DefaultName';");

        // 1. Check existing rows - should have the default value
        let result_existing = execute_query_assert_ok(&engine, "SELECT id, name FROM test_add_not_null_default ORDER BY id;");
        assert_eq!(result_existing.columns(), vec!["id", "name"]);
        let rows_existing = result_existing.rows();
        assert_eq!(rows_existing.len(), 2);
        assert_eq!(rows_existing[0].get("name"), Some(&DataValue::Text("DefaultName".to_string())));
        assert_eq!(rows_existing[1].get("name"), Some(&DataValue::Text("DefaultName".to_string())));

        // 2. Insert a new row providing the new column
        execute_query_assert_ok(&engine, "INSERT INTO test_add_not_null_default (id, name) VALUES (3, 'SpecificName');");
        let result_new_provided = execute_query_assert_ok(&engine, "SELECT name FROM test_add_not_null_default WHERE id = 3;");
        assert_eq!(result_new_provided.rows()[0].get("name"), Some(&DataValue::Text("SpecificName".to_string())));

        // 3. Insert a new row omitting the new column (should use default, and satisfy NOT NULL)
        execute_query_assert_ok(&engine, "INSERT INTO test_add_not_null_default (id) VALUES (4);");
        let result_new_defaulted = execute_query_assert_ok(&engine, "SELECT name FROM test_add_not_null_default WHERE id = 4;");
        assert_eq!(result_new_defaulted.rows()[0].get("name"), Some(&DataValue::Text("DefaultName".to_string())));

        Ok(())
    }

    // TODO: Add test_add_column_not_null_no_default_fails_on_non_empty_table()
    // TODO: Add test_add_column_not_null_no_default_succeeds_on_empty_table()
    // TODO: Add test_add_column_already_exists_fails()
}

// TODO: Add mod alter_table_drop_column_tests { ... }
// TODO: Add mod alter_table_rename_column_tests { ... }
// TODO: Add mod alter_table_general_error_tests { ... } 