// Schema Validation Integration Tests
//
// This module tests schema validation and type checking

use anyhow::{Result, anyhow};
use bayundb::catalog::{Catalog, Table, Column, DataType, TypeValidator};
use bayundb::query::executor::result::{Row, DataValue};
use bayundb::query::parser::ast::{Expression, Value, Operator, ColumnReference};

#[test]
fn test_type_validation() {
    // Generate a unique table name for this test run
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let test_table_name = format!("validation_test_{}", timestamp);
    
    // Create a test catalog and table
    {
        let catalog = Catalog::instance();
        let catalog_mut = catalog.write().unwrap();
        
        // Create a test table with specific columns
        let columns = vec![
            Column::new("id".to_string(), DataType::Integer, false, true, None),
            Column::new("name".to_string(), DataType::Text, false, false, None),
            Column::new("active".to_string(), DataType::Boolean, true, false, None),
            Column::new("score".to_string(), DataType::Float, true, false, None),
        ];
        
        let table = Table::new(test_table_name.clone(), columns);
        catalog_mut.create_table(table.clone()).unwrap();
        
        // The write lock is released when catalog_mut goes out of scope
    }
    
    // Test value validation
    {
        let catalog = Catalog::instance();
        let catalog_read = catalog.read().unwrap();
        let table = catalog_read.get_table(&test_table_name).unwrap();
        
        // Create a valid row
        let mut row = Row::new();
        row.set("id".to_string(), DataValue::Integer(1));
        row.set("name".to_string(), DataValue::Text("Test".to_string()));
        row.set("active".to_string(), DataValue::Boolean(true));
        row.set("score".to_string(), DataValue::Float(95.5));
        
        // Get values with names for validation
        let values: Vec<(&String, &DataValue)> = row.values_with_names().collect();
        
        // Test valid row
        assert!(TypeValidator::validate_row(&values, &table).is_ok());
        
        // Test missing required column
        let mut invalid_row = Row::new();
        invalid_row.set("id".to_string(), DataValue::Integer(2));
        // Missing 'name' which is non-nullable
        invalid_row.set("active".to_string(), DataValue::Boolean(false));
        
        let invalid_values: Vec<(&String, &DataValue)> = invalid_row.values_with_names().collect();
        assert!(TypeValidator::validate_row(&invalid_values, &table).is_err());
        
        // Test wrong data type
        let mut type_error_row = Row::new();
        type_error_row.set("id".to_string(), DataValue::Text("not an integer".to_string())); // Wrong type
        type_error_row.set("name".to_string(), DataValue::Text("Test".to_string()));
        
        let type_error_values: Vec<(&String, &DataValue)> = type_error_row.values_with_names().collect();
        assert!(TypeValidator::validate_row(&type_error_values, &table).is_err());
        
        // The read lock is released when catalog_read goes out of scope
    }
    
    // Test expression type checking
    {
        let catalog = Catalog::instance();
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
        
        let expr1_type = TypeValidator::get_expression_type(&expr1, &table);
        assert!(expr1_type.is_ok());
        assert_eq!(expr1_type.unwrap(), DataType::Integer);
        
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
        
        let expr2_type = TypeValidator::get_expression_type(&expr2, &table);
        assert!(expr2_type.is_err());
        
        // Valid comparison: id > 10 (Integer > Integer)
        let expr3 = Expression::BinaryOp {
            left: Box::new(Expression::Column(ColumnReference {
                table: None,
                name: "id".to_string(),
            })),
            op: Operator::GreaterThan,
            right: Box::new(Expression::Literal(Value::Integer(10))),
        };
        
        let expr3_type = TypeValidator::get_expression_type(&expr3, &table);
        assert!(expr3_type.is_ok());
        assert_eq!(expr3_type.unwrap(), DataType::Boolean);
        
        // The read lock is released when catalog_read goes out of scope
    }
    
    // Test type conversion
    {
        // Integer to Float (should succeed)
        let int_val = DataValue::Integer(42);
        let float_result = TypeValidator::convert_to_type(int_val, &DataType::Float);
        assert!(float_result.is_ok());
        if let DataValue::Float(f) = float_result.unwrap() {
            assert_eq!(f, 42.0);
        } else {
            panic!("Expected Float result");
        }
        
        // Text to Integer (should succeed if valid)
        let text_val = DataValue::Text("123".to_string());
        let int_result = TypeValidator::convert_to_type(text_val, &DataType::Integer);
        assert!(int_result.is_ok());
        if let DataValue::Integer(i) = int_result.unwrap() {
            assert_eq!(i, 123);
        } else {
            panic!("Expected Integer result");
        }
        
        // Text to Integer (should fail if invalid)
        let text_val = DataValue::Text("not a number".to_string());
        let int_result = TypeValidator::convert_to_type(text_val, &DataType::Integer);
        assert!(int_result.is_err());
    }
    
    // No cleanup needed - we used a unique table name for this test run
} 