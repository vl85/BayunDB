use bayundb::catalog::{Catalog, Table, Column, DataType, TypeValidator};
use bayundb::query::executor::result::{Row, DataValue};
use bayundb::query::parser::ast::{Expression, Value, Operator, ColumnReference};
use std::time::Instant;

// Helper function to print elapsed time with message
fn log_step(step_name: &str, start: Instant) {
    println!("{} - Elapsed: {:?}", step_name, start.elapsed());
}

// Helper to print with timestamp
fn debug_print(msg: &str) {
    let now = Instant::now();
    println!("[{:?}] {}", now.elapsed(), msg);
}

// Main debug function
fn main() {
    let debug_start = Instant::now();
    debug_print("Starting validation debug program");
    
    // Generate a unique table name
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let test_table_name = format!("validation_test_{}", timestamp);
    
    debug_print(&format!("Using table name: {}", test_table_name));
    
    // STEP 1: Create a test table
    debug_print("STEP 1: Creating test table");
    
    let catalog = Catalog::instance();
    debug_print("Got catalog instance");
    
    let catalog_write_start = Instant::now();
    debug_print("Acquiring write lock on catalog");
    
    let catalog_mut = match catalog.write() {
        Ok(guard) => {
            debug_print(&format!("Acquired write lock after {:?}", catalog_write_start.elapsed()));
            guard
        },
        Err(e) => {
            debug_print(&format!("Failed to acquire write lock: {:?}", e));
            panic!("Failed to acquire write lock on catalog");
        }
    };

    // Create a test table with specific columns
    let columns = vec![
        Column::new("id".to_string(), DataType::Integer, false, true, None),
        Column::new("name".to_string(), DataType::Text, false, false, None),
        Column::new("active".to_string(), DataType::Boolean, true, false, None),
        Column::new("score".to_string(), DataType::Float, true, false, None),
    ];
    
    debug_print("Created columns");
    
    let table = Table::new(test_table_name.clone(), columns);
    debug_print("Created table object");
    
    match catalog_mut.create_table(table.clone()) {
        Ok(_) => debug_print("Table created successfully"),
        Err(e) => {
            debug_print(&format!("Failed to create table: {}", e));
            panic!("Failed to create table");
        }
    }
    
    // Important: Release the write lock
    debug_print("Dropping catalog_mut to release write lock");
    drop(catalog_mut);
    debug_print("Write lock should be released now");
    
    log_step("Table creation complete", debug_start);

    // STEP 2: Test value validation
    debug_print("STEP 2: Testing value validation");
    
    let catalog_read_start = Instant::now();
    debug_print("Acquiring read lock on catalog");
    
    let catalog = Catalog::instance();
    let catalog_read = match catalog.read() {
        Ok(guard) => {
            debug_print(&format!("Acquired read lock after {:?}", catalog_read_start.elapsed()));
            guard
        },
        Err(e) => {
            debug_print(&format!("Failed to acquire read lock: {:?}", e));
            panic!("Failed to acquire read lock on catalog");
        }
    };
    
    let table_lookup_start = Instant::now();
    debug_print(&format!("Looking up table '{}'", test_table_name));
    
    let table = match catalog_read.get_table(&test_table_name) {
        Some(t) => {
            debug_print(&format!("Found table after {:?}", table_lookup_start.elapsed()));
            t
        },
        None => {
            debug_print("Table not found!");
            panic!("Table not found");
        }
    };
    
    // Create a valid row
    let mut row = Row::new();
    row.set("id".to_string(), DataValue::Integer(1));
    row.set("name".to_string(), DataValue::Text("Test".to_string()));
    row.set("active".to_string(), DataValue::Boolean(true));
    row.set("score".to_string(), DataValue::Float(95.5));
    
    debug_print("Created test row");
    
    // Get values with names for validation
    let values_start = Instant::now();
    debug_print("Collecting row values with names");
    let values: Vec<(&String, &DataValue)> = row.values_with_names().collect();
    debug_print(&format!("Collected values after {:?}", values_start.elapsed()));
    
    // Test valid row
    let validation_start = Instant::now();
    debug_print("Validating row against table schema");
    
    match TypeValidator::validate_row(&values, &table) {
        Ok(_) => debug_print("Row validation succeeded"),
        Err(e) => {
            debug_print(&format!("Row validation failed: {:?}", e));
            panic!("Row validation failed unexpectedly");
        }
    }
    
    debug_print(&format!("Validation completed after {:?}", validation_start.elapsed()));
    
    // Release read lock before continuing
    debug_print("Dropping catalog_read to release read lock");
    drop(catalog_read);
    debug_print("Read lock should be released now");
    
    log_step("Value validation complete", debug_start);

    // STEP 3: Test expression type checking
    debug_print("STEP 3: Testing expression type checking");
    
    // Re-acquire read lock
    let catalog = Catalog::instance();
    debug_print("Acquiring read lock for expression tests");
    let catalog_read = catalog.read().unwrap();
    let table = catalog_read.get_table(&test_table_name).unwrap();
    
    // Valid expression: id + 5 (Integer + Integer)
    let expr1 = Expression::BinaryOp {
        left: Box::new(Expression::Column(ColumnReference {
            table: None,
            name: "id".to_string(),
        })),
        op: Operator::Plus,
        right: Box::new(Expression::Literal(Value::Integer(5))),
    };
    
    debug_print("Checking expression: id + 5");
    let expr1_start = Instant::now();
    let expr1_type = TypeValidator::get_expression_type(&expr1, &table);
    
    match &expr1_type {
        Ok(t) => debug_print(&format!("Expression type: {:?}", t)),
        Err(e) => debug_print(&format!("Expression type error: {:?}", e)),
    }
    
    debug_print(&format!("Expression check completed after {:?}", expr1_start.elapsed()));
    assert!(expr1_type.is_ok());
    
    // Invalid expression: id + name (Integer + Text)
    let expr2 = Expression::BinaryOp {
        left: Box::new(Expression::Column(ColumnReference {
            table: None,
            name: "id".to_string(),
        })),
        op: Operator::Plus,
        right: Box::new(Expression::Column(ColumnReference {
            table: None, 
            name: "name".to_string(),
        })),
    };
    
    debug_print("Checking expression: id + name (should fail)");
    let expr2_start = Instant::now();
    let expr2_type = TypeValidator::get_expression_type(&expr2, &table);
    
    match &expr2_type {
        Ok(t) => debug_print(&format!("Expression type (unexpected success): {:?}", t)),
        Err(e) => debug_print(&format!("Expression type error (expected): {:?}", e)),
    }
    
    debug_print(&format!("Expression check completed after {:?}", expr2_start.elapsed()));
    assert!(expr2_type.is_err());
    
    // Release read lock before continuing
    debug_print("Dropping catalog_read to release read lock");
    drop(catalog_read);
    
    log_step("Expression type checking complete", debug_start);

    // STEP 4: Test type conversion
    debug_print("STEP 4: Testing type conversion");
    
    // Test conversions independently of table
    
    // Integer to Float (should succeed)
    debug_print("Converting Integer to Float");
    let int_val = DataValue::Integer(42);
    let float_result = TypeValidator::convert_to_type(int_val, &DataType::Float);
    
    match &float_result {
        Ok(v) => debug_print(&format!("Conversion result: {:?}", v)),
        Err(e) => debug_print(&format!("Conversion error: {:?}", e)),
    }
    
    assert!(float_result.is_ok());
    
    // Text to Integer (should succeed if valid)
    debug_print("Converting Text '123' to Integer");
    let text_val = DataValue::Text("123".to_string());
    let int_result = TypeValidator::convert_to_type(text_val, &DataType::Integer);
    
    match &int_result {
        Ok(v) => debug_print(&format!("Conversion result: {:?}", v)),
        Err(e) => debug_print(&format!("Conversion error: {:?}", e)),
    }
    
    assert!(int_result.is_ok());
    
    // Text to Integer (should fail if invalid)
    debug_print("Converting Text 'not a number' to Integer (should fail)");
    let text_val = DataValue::Text("not a number".to_string());
    let int_result = TypeValidator::convert_to_type(text_val, &DataType::Integer);
    
    match &int_result {
        Ok(v) => debug_print(&format!("Conversion result (unexpected success): {:?}", v)),
        Err(e) => debug_print(&format!("Conversion error (expected): {:?}", e)),
    }
    
    assert!(int_result.is_err());
    
    log_step("Type conversion tests complete", debug_start);
    
    // Final timing summary
    debug_print("ALL TESTS COMPLETED SUCCESSFULLY");
    println!("Total execution time: {:?}", debug_start.elapsed());
} 