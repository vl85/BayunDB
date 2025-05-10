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
use std::collections::HashMap;
use std::sync::RwLock;

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
        db_path,
        log_manager.clone(),
    )?);
    
    // Use a new catalog for each test to ensure isolation
    let catalog = Arc::new(RwLock::new(Catalog::new()));
    
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
    use bayundb::query::executor::result::{DataValue, QueryError};

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

    #[test]
    fn test_add_column_not_null_no_default_fails_on_non_empty_table() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;
        execute_query_assert_ok(&engine, "CREATE TABLE test_add_nn_nd_fail (id INT);");
        execute_query_assert_ok(&engine, "INSERT INTO test_add_nn_nd_fail VALUES (1);");

        let result = engine.execute_query("ALTER TABLE test_add_nn_nd_fail ADD COLUMN new_col TEXT NOT NULL;");
        assert!(result.is_err(), "Expected error when adding NOT NULL column without DEFAULT to non-empty table");
        match result.unwrap_err() {
            QueryError::ExecutionError(msg) => {
                assert!(msg.contains("Cannot ADD non-nullable column"));
                assert!(msg.contains("without a DEFAULT value"));
                assert!(msg.contains("'new_col'"), "Message '{}' did not contain column name 'new_col'", msg);
                assert!(msg.contains("'test_add_nn_nd_fail'"), "Message '{}' did not contain table name 'test_add_nn_nd_fail'", msg);
            }
            e => panic!("Expected ExecutionError for missing default on NOT NULL, got {:?}", e),
        }
        Ok(())
    }

    #[test]
    fn test_add_column_not_null_no_default_succeeds_on_empty_table() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;
        execute_query_assert_ok(&engine, "CREATE TABLE test_add_nn_nd_empty (id INT);");
        // Table is empty
        execute_query_assert_ok(&engine, "ALTER TABLE test_add_nn_nd_empty ADD COLUMN new_col TEXT NOT NULL;");
        
        // Insert should now require the new column or fail if it's not provided and has no default (which it doesn't)
        let insert_fail_result = engine.execute_query("INSERT INTO test_add_nn_nd_empty (id) VALUES (1);");
        assert!(insert_fail_result.is_err(), "Insert should fail if NOT NULL column 'new_col' is not provided");
        // This error might come from catalog validation during INSERT planning/execution, not from ADD COLUMN itself.
        // The exact error type might vary. For now, we check it's an error.

        execute_query_assert_ok(&engine, "INSERT INTO test_add_nn_nd_empty (id, new_col) VALUES (2, 'NonNullValue');");
        let select_result = execute_query_assert_ok(&engine, "SELECT new_col FROM test_add_nn_nd_empty WHERE id = 2;");
        assert_eq!(select_result.rows()[0].get("new_col"), Some(&DataValue::Text("NonNullValue".to_string())));
        Ok(())
    }

    #[test]
    fn test_add_column_already_exists_fails() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;
        execute_query_assert_ok(&engine, "CREATE TABLE test_add_exists (id INT, name TEXT);");
        let result = engine.execute_query("ALTER TABLE test_add_exists ADD COLUMN name INT;"); // Attempt to add existing 'name'
        assert!(result.is_err(), "Expected error when adding a column that already exists.");
        match result.unwrap_err() {
            QueryError::CatalogError(msg) => {
                assert!(msg.to_lowercase().contains("column") && msg.to_lowercase().contains("name") && msg.to_lowercase().contains("exists"));
            }
            e => panic!("Expected CatalogError for column already exists, got {:?}", e),
        }
        Ok(())
    }
}

#[cfg(test)]
mod alter_table_drop_column_tests {
    use super::*;
    use bayundb::query::executor::result::{DataValue, QueryError};

    #[test]
    fn test_drop_column_basic() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;
        execute_query_assert_ok(&engine, "CREATE TABLE test_drop_basic (id INT, name TEXT, age INT);");
        execute_query_assert_ok(&engine, "INSERT INTO test_drop_basic VALUES (1, 'Alice', 30);");
        execute_query_assert_ok(&engine, "INSERT INTO test_drop_basic VALUES (2, 'Bob', 25);");

        execute_query_assert_ok(&engine, "ALTER TABLE test_drop_basic DROP COLUMN name;");

        let result = execute_query_assert_ok(&engine, "SELECT id, age FROM test_drop_basic ORDER BY id;");
        assert_eq!(result.columns(), vec!["id", "age"]);
        let rows = result.rows();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("id"), Some(&DataValue::Integer(1)));
        assert_eq!(rows[0].get("age"), Some(&DataValue::Integer(30)));
        assert_eq!(rows[1].get("id"), Some(&DataValue::Integer(2)));
        assert_eq!(rows[1].get("age"), Some(&DataValue::Integer(25)));

        // Verify that selecting the dropped column fails or returns nothing relevant (parser/planner should ideally prevent this).
        // For now, we check that the column is not in the new result set.
        let res_after_drop = execute_query_assert_ok(&engine, "SELECT * FROM test_drop_basic WHERE id = 1;");
        assert!(res_after_drop.rows()[0].get("name").is_none(), "'name' column should not exist after drop.");

        // Insert new data, should only accept new schema
        execute_query_assert_ok(&engine, "INSERT INTO test_drop_basic (id, age) VALUES (3, 40);");
        let result_new = execute_query_assert_ok(&engine, "SELECT age FROM test_drop_basic WHERE id = 3;");
        assert_eq!(result_new.rows()[0].get("age"), Some(&DataValue::Integer(40)));

        Ok(())
    }

    #[test]
    fn test_drop_middle_column() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;
        execute_query_assert_ok(&engine, "CREATE TABLE test_drop_middle (id INT, name TEXT, age INT, city TEXT);");
        execute_query_assert_ok(&engine, "INSERT INTO test_drop_middle VALUES (1, 'Eve', 28, 'London');");
        
        execute_query_assert_ok(&engine, "ALTER TABLE test_drop_middle DROP COLUMN age;");

        let result = execute_query_assert_ok(&engine, "SELECT id, name, city FROM test_drop_middle;");
        assert_eq!(result.columns(), vec!["id", "name", "city"]);
        assert_eq!(result.rows()[0].get("name"), Some(&DataValue::Text("Eve".to_string())));
        assert_eq!(result.rows()[0].get("city"), Some(&DataValue::Text("London".to_string())));

        Ok(())
    }

    #[test]
    fn test_drop_last_column() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;
        execute_query_assert_ok(&engine, "CREATE TABLE test_drop_last (id INT, name TEXT);");
        execute_query_assert_ok(&engine, "INSERT INTO test_drop_last VALUES (1, 'LastCol');");
        
        execute_query_assert_ok(&engine, "ALTER TABLE test_drop_last DROP COLUMN name;");
        
        let result = execute_query_assert_ok(&engine, "SELECT id FROM test_drop_last;");
        assert_eq!(result.columns(), vec!["id"]);
        assert_eq!(result.rows()[0].get("id"), Some(&DataValue::Integer(1)));

        // Attempting to drop the final column should now result in a specific error
        // The SELECT * from the original version of the test was here.
        // Let's verify the state of the table just before trying to drop the last column.
        let result_before_final_drop = execute_query_assert_ok(&engine, "SELECT * FROM test_drop_last;");
        assert_eq!(result_before_final_drop.columns().len(), 1); // This was the failing line (assert_eq!(..., 0))
        assert_eq!(result_before_final_drop.columns()[0], "id");
        assert_eq!(result_before_final_drop.rows().len(), 1); 

        let drop_last_col_result = engine.execute_query("ALTER TABLE test_drop_last DROP COLUMN id;");
        assert!(drop_last_col_result.is_err(), "Expected error when dropping the last column.");
        match drop_last_col_result.unwrap_err() {
            QueryError::ExecutionError(msg) => {
                assert!(msg.contains("Cannot drop the last column"));
                assert!(msg.contains("Use DROP TABLE instead"));
            }
            e => panic!("Expected ExecutionError, got {:?}", e),
        }

        Ok(())
    }

    #[test]
    fn test_drop_non_existent_column_fails() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;
        execute_query_assert_ok(&engine, "CREATE TABLE test_drop_non_exist (id INT);");
        let result = engine.execute_query("ALTER TABLE test_drop_non_exist DROP COLUMN missing_col;");
        assert!(result.is_err());
        match result.unwrap_err() {
            QueryError::ColumnNotFound(details) => {
                assert!(details.contains("missing_col"));
                assert!(details.contains("test_drop_non_exist"));
            }
            e => panic!("Expected ColumnNotFound error, got {:?}", e),
        }
        Ok(())
    }
}

#[cfg(test)]
mod alter_table_rename_column_tests {
    use super::*;
    use bayundb::query::executor::result::{DataValue, QueryError};

    #[test]
    fn test_rename_column_basic() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;
        execute_query_assert_ok(&engine, "CREATE TABLE test_rename_basic (id INT, old_name TEXT);");
        execute_query_assert_ok(&engine, "INSERT INTO test_rename_basic VALUES (1, 'InitialName');");

        execute_query_assert_ok(&engine, "ALTER TABLE test_rename_basic RENAME COLUMN old_name TO new_name;");

        let result = execute_query_assert_ok(&engine, "SELECT id, new_name FROM test_rename_basic;");
        assert_eq!(result.columns(), vec!["id", "new_name"]);
        assert_eq!(result.rows()[0].get("new_name"), Some(&DataValue::Text("InitialName".to_string())));

        // Selecting by old name should fail (or not return the column)
        // Depending on strictness, this could be a parse error or planner error.
        // For now, check that it's not in the * projection.
        let result_star = execute_query_assert_ok(&engine, "SELECT * FROM test_rename_basic;");
        assert!(result_star.rows()[0].get("old_name").is_none());
        assert!(result_star.rows()[0].get("new_name").is_some());

        Ok(())
    }

    #[test]
    fn test_rename_column_to_existing_name_fails() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;
        execute_query_assert_ok(&engine, "CREATE TABLE test_rename_conflict (id INT, name TEXT, description TEXT);");
        let result = engine.execute_query("ALTER TABLE test_rename_conflict RENAME COLUMN description TO name;");
        assert!(result.is_err());
        // Expecting a specific error related to duplicate column name from catalog or execution engine
        match result.unwrap_err() {
            QueryError::DuplicateColumn(col_name) => {
                assert_eq!(col_name, "name");
            }
            e => panic!("Expected DuplicateColumn error, got {:?}", e),
        }
        Ok(())
    }

    #[test]
    fn test_rename_non_existent_column_fails() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;
        execute_query_assert_ok(&engine, "CREATE TABLE test_rename_non_exist (id INT);");
        let result = engine.execute_query("ALTER TABLE test_rename_non_exist RENAME COLUMN missing_col TO new_missing_col;");
        assert!(result.is_err());
        match result.unwrap_err() {
            QueryError::CatalogError(details) => {
                assert!(details.contains("missing_col"));
                assert!(details.contains("does not exist"));
                assert!(details.contains("test_rename_non_exist"));
            }
            e => panic!("Expected CatalogError for renaming non-existent column, got {:?}", e),
        }
        Ok(())
    }
}

#[cfg(test)]
mod alter_table_general_error_tests {
    use super::*;
    use bayundb::query::executor::result::QueryError;

    #[test]
    fn test_alter_non_existent_table_fails() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;
        
        let add_col_res = engine.execute_query("ALTER TABLE non_existent_table ADD COLUMN col1 INT;");
        assert!(add_col_res.is_err());
        match add_col_res.unwrap_err() {
            QueryError::TableNotFound(name) => assert_eq!(name, "non_existent_table"),
            e => panic!("Expected TableNotFound for ADD COLUMN, got {:?}", e),
        }

        let drop_col_res = engine.execute_query("ALTER TABLE non_existent_table DROP COLUMN col1;");
        assert!(drop_col_res.is_err());
         match drop_col_res.unwrap_err() {
            QueryError::TableNotFound(name) => assert_eq!(name, "non_existent_table"),
            e => panic!("Expected TableNotFound for DROP COLUMN, got {:?}", e),
        }

        let rename_col_res = engine.execute_query("ALTER TABLE non_existent_table RENAME COLUMN col1 TO col2;");
        assert!(rename_col_res.is_err());
        match rename_col_res.unwrap_err() {
            QueryError::TableNotFound(name) => assert_eq!(name, "non_existent_table"),
            e => panic!("Expected TableNotFound for RENAME COLUMN, got {:?}", e),
        }

        let alter_type_res = engine.execute_query("ALTER TABLE non_existent_table ALTER COLUMN col1 TYPE TEXT;");
        assert!(alter_type_res.is_err());
        match alter_type_res.unwrap_err() {
            QueryError::TableNotFound(name) => assert_eq!(name, "non_existent_table"),
            e => panic!("Expected TableNotFound for ALTER COLUMN TYPE, got {:?}", e),
        }
        Ok(())
    }
}

#[cfg(test)]
mod alter_table_alter_column_type_tests {
    use super::*;
    use bayundb::query::executor::result::{DataValue, QueryError};

    #[test]
    fn test_alter_column_type_int_to_float() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;

        execute_query_assert_ok(&engine, "CREATE TABLE type_change_test (id INT, val INT);");
        execute_query_assert_ok(&engine, "INSERT INTO type_change_test VALUES (1, 10);");
        execute_query_assert_ok(&engine, "INSERT INTO type_change_test VALUES (2, -5);");
        execute_query_assert_ok(&engine, "INSERT INTO type_change_test VALUES (3, 0);");

        // Alter column 'val' from INT to FLOAT
        execute_query_assert_ok(&engine, "ALTER TABLE type_change_test ALTER COLUMN val TYPE FLOAT;");

        // Verify data conversion and schema
        let result = execute_query_assert_ok(&engine, "SELECT id, val FROM type_change_test ORDER BY id;");
        assert_eq!(result.columns(), vec!["id", "val"]);
        let rows = result.rows();
        assert_eq!(rows.len(), 3);

        assert_eq!(rows[0].get("id"), Some(&DataValue::Integer(1)));
        assert_eq!(rows[0].get("val"), Some(&DataValue::Float(10.0)));

        assert_eq!(rows[1].get("id"), Some(&DataValue::Integer(2)));
        assert_eq!(rows[1].get("val"), Some(&DataValue::Float(-5.0)));

        assert_eq!(rows[2].get("id"), Some(&DataValue::Integer(3)));
        assert_eq!(rows[2].get("val"), Some(&DataValue::Float(0.0)));

        // Insert new data with float type
        execute_query_assert_ok(&engine, "INSERT INTO type_change_test (id, val) VALUES (4, 123.45);");
        let result_new = execute_query_assert_ok(&engine, "SELECT val FROM type_change_test WHERE id = 4;");
        assert_eq!(result_new.rows()[0].get("val"), Some(&DataValue::Float(123.45)));

        // Check catalog (conceptual - actual check might be more direct if catalog API allows type inspection easily)
        // For now, relying on behavior of INSERT and SELECT with the new type.

        Ok(())
    }

    #[test]
    fn test_alter_column_type_text_to_int_valid() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;
        execute_query_assert_ok(&engine, "CREATE TABLE tt_text_to_int (val TEXT);");
        execute_query_assert_ok(&engine, "INSERT INTO tt_text_to_int VALUES ('123');");
        execute_query_assert_ok(&engine, "INSERT INTO tt_text_to_int VALUES ('-456');");
        execute_query_assert_ok(&engine, "INSERT INTO tt_text_to_int VALUES (NULL);");
        execute_query_assert_ok(&engine, "INSERT INTO tt_text_to_int VALUES ('0');");
        execute_query_assert_ok(&engine, "ALTER TABLE tt_text_to_int ALTER COLUMN val TYPE INT;");

        let result = execute_query_assert_ok(&engine, "SELECT val FROM tt_text_to_int ORDER BY CASE WHEN val IS NULL THEN 0 ELSE 1 END, val;"); // Order NULLs first
        let rows = result.rows();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].get("val"), Some(&DataValue::Null));
        assert_eq!(rows[1].get("val"), Some(&DataValue::Integer(-456)));
        assert_eq!(rows[2].get("val"), Some(&DataValue::Integer(0)));
        assert_eq!(rows[3].get("val"), Some(&DataValue::Integer(123)));
        Ok(())
    }

    #[test]
    fn test_alter_column_type_text_to_int_invalid_fails() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;
        execute_query_assert_ok(&engine, "CREATE TABLE tt_text_to_int_fail (val TEXT);");
        execute_query_assert_ok(&engine, "INSERT INTO tt_text_to_int_fail VALUES ('123');");
        execute_query_assert_ok(&engine, "INSERT INTO tt_text_to_int_fail VALUES ('abc');"); // Invalid int

        let alter_result = engine.execute_query("ALTER TABLE tt_text_to_int_fail ALTER COLUMN val TYPE INT;");
        assert!(alter_result.is_err());
        match alter_result.unwrap_err() {
            QueryError::TypeError(msg) => {
                assert!(msg.contains("Cannot convert Text 'abc' to Integer"));
            }
            e => panic!("Expected TypeError, got {:?}", e),
        }
        Ok(())
    }

    #[test]
    fn test_alter_column_type_int_to_text() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;
        execute_query_assert_ok(&engine, "CREATE TABLE tt_int_to_text (val INT);");
        execute_query_assert_ok(&engine, "INSERT INTO tt_int_to_text VALUES (123);");
        execute_query_assert_ok(&engine, "INSERT INTO tt_int_to_text VALUES (-456);");
        execute_query_assert_ok(&engine, "INSERT INTO tt_int_to_text VALUES (0);");
        execute_query_assert_ok(&engine, "ALTER TABLE tt_int_to_text ALTER COLUMN val TYPE TEXT;");

        let result = execute_query_assert_ok(&engine, "SELECT val FROM tt_int_to_text ORDER BY val;"); // Text sort
        let rows = result.rows();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get("val"), Some(&DataValue::Text("-456".to_string())));
        assert_eq!(rows[1].get("val"), Some(&DataValue::Text("0".to_string())));
        assert_eq!(rows[2].get("val"), Some(&DataValue::Text("123".to_string())));
        Ok(())
    }

    #[test]
    fn test_alter_column_type_float_to_int_truncation() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;
        execute_query_assert_ok(&engine, "CREATE TABLE tt_float_to_int (val FLOAT);");
        execute_query_assert_ok(&engine, "INSERT INTO tt_float_to_int VALUES (123.789);");
        execute_query_assert_ok(&engine, "INSERT INTO tt_float_to_int VALUES (-456.123);");
        execute_query_assert_ok(&engine, "INSERT INTO tt_float_to_int VALUES (0.0);");
        execute_query_assert_ok(&engine, "INSERT INTO tt_float_to_int VALUES (NULL);");
        execute_query_assert_ok(&engine, "ALTER TABLE tt_float_to_int ALTER COLUMN val TYPE INT;");

        let result = execute_query_assert_ok(&engine, "SELECT val FROM tt_float_to_int ORDER BY CASE WHEN val IS NULL THEN 0 ELSE 1 END, val;");
        let rows = result.rows();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].get("val"), Some(&DataValue::Null));
        assert_eq!(rows[1].get("val"), Some(&DataValue::Integer(-456))); // Truncated
        assert_eq!(rows[2].get("val"), Some(&DataValue::Integer(0)));
        assert_eq!(rows[3].get("val"), Some(&DataValue::Integer(123))); // Truncated
        Ok(())
    }

    #[test]
    fn test_alter_column_type_to_same_type() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;
        execute_query_assert_ok(&engine, "CREATE TABLE tt_same_type (val INT);");
        execute_query_assert_ok(&engine, "INSERT INTO tt_same_type VALUES (10);");
        execute_query_assert_ok(&engine, "ALTER TABLE tt_same_type ALTER COLUMN val TYPE INT;"); // Alter to same type

        let result = execute_query_assert_ok(&engine, "SELECT val FROM tt_same_type;");
        assert_eq!(result.rows()[0].get("val"), Some(&DataValue::Integer(10)));
        Ok(())
    }

    #[test]
    fn test_alter_column_type_bool_to_text() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;
        execute_query_assert_ok(&engine, "CREATE TABLE tt_bool_to_text (val BOOLEAN);");
        execute_query_assert_ok(&engine, "INSERT INTO tt_bool_to_text VALUES (TRUE);");
        execute_query_assert_ok(&engine, "INSERT INTO tt_bool_to_text VALUES (FALSE);");
        execute_query_assert_ok(&engine, "INSERT INTO tt_bool_to_text VALUES (NULL);");
        execute_query_assert_ok(&engine, "ALTER TABLE tt_bool_to_text ALTER COLUMN val TYPE TEXT;");

        let result = execute_query_assert_ok(&engine, "SELECT val FROM tt_bool_to_text ORDER BY val;");
        let rows = result.rows();
        assert_eq!(rows.len(), 3);
        // Order will be alphabetical: "false", "true", NULL (or however NULLs sort by default string comparison)
        // Assuming NULLs first for this check based on typical database behavior or explicit sort.
        // Let's check for presence and then individual converted values if order is tricky.
        let mut values_found = rows.iter().map(|r| r.get("val").unwrap().clone()).collect::<Vec<_>>();
        values_found.sort_by(|a,b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal) ); // Approximate sort for test

        assert!(values_found.contains(&DataValue::Text("true".to_string())));
        assert!(values_found.contains(&DataValue::Text("false".to_string())));
        assert!(values_found.contains(&DataValue::Null));
        Ok(())
    }

    #[test]
    fn test_alter_column_type_text_to_bool_valid() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;
        execute_query_assert_ok(&engine, "CREATE TABLE tt_text_to_bool (val TEXT);");
        execute_query_assert_ok(&engine, "INSERT INTO tt_text_to_bool VALUES ('true');");
        execute_query_assert_ok(&engine, "INSERT INTO tt_text_to_bool VALUES ('FALSE');");
        execute_query_assert_ok(&engine, "INSERT INTO tt_text_to_bool VALUES ('t');");
        execute_query_assert_ok(&engine, "INSERT INTO tt_text_to_bool VALUES ('F');");
        execute_query_assert_ok(&engine, "INSERT INTO tt_text_to_bool VALUES ('1');");
        execute_query_assert_ok(&engine, "INSERT INTO tt_text_to_bool VALUES ('0');");
        execute_query_assert_ok(&engine, "INSERT INTO tt_text_to_bool VALUES (NULL);");
        execute_query_assert_ok(&engine, "ALTER TABLE tt_text_to_bool ALTER COLUMN val TYPE BOOLEAN;");

        let result = execute_query_assert_ok(&engine, "SELECT val FROM tt_text_to_bool;");
        let rows = result.rows();
        assert_eq!(rows.len(), 7);
        // Values are not ordered by select, so check against input order or collect and check presence
        let mut bool_values = HashMap::new();
        for row in rows {
            let val = row.get("val").unwrap();
            *bool_values.entry(val.clone()).or_insert(0) += 1;
        }
        assert_eq!(bool_values.get(&DataValue::Boolean(true)), Some(&3)); // true, t, 1
        assert_eq!(bool_values.get(&DataValue::Boolean(false)), Some(&3)); // FALSE, F, 0
        assert_eq!(bool_values.get(&DataValue::Null), Some(&1));
        Ok(())
    }

    #[test]
    fn test_alter_column_type_text_to_bool_invalid_fails() -> Result<()> {
        let (engine, _temp_dir) = setup_test_db()?;
        execute_query_assert_ok(&engine, "CREATE TABLE tt_text_to_bool_fail (val TEXT);");
        execute_query_assert_ok(&engine, "INSERT INTO tt_text_to_bool_fail VALUES ('true');");
        execute_query_assert_ok(&engine, "INSERT INTO tt_text_to_bool_fail VALUES ('notabool');");

        let alter_result = engine.execute_query("ALTER TABLE tt_text_to_bool_fail ALTER COLUMN val TYPE BOOLEAN;");
        assert!(alter_result.is_err());
        match alter_result.unwrap_err() {
            QueryError::TypeError(msg) => {
                assert!(msg.contains("Cannot convert Text 'notabool' to Boolean"));
            }
            e => panic!("Expected TypeError, got {:?}", e),
        }
        Ok(())
    }
} 