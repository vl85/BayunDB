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
    let table_name = format!("validation_test_{}", timestamp);
    
    // Create a test catalog and get access to it
    let catalog = Catalog::instance();
    
    // Create a test table with columns of different types
    let mut columns = Vec::new();
    columns.push(Column::new("id".to_string(), DataType::Integer, true, true, None));
    columns.push(Column::new("name".to_string(), DataType::Text, false, false, None));
    columns.push(Column::new("age".to_string(), DataType::Integer, false, false, None));
    columns.push(Column::new("salary".to_string(), DataType::Float, false, false, None));
    columns.push(Column::new("is_active".to_string(), DataType::Boolean, false, false, None));
    
    let table = Table::new(table_name.clone(), columns);
    
    // Add the table to the catalog
    {
        let mut catalog_guard = catalog.write().unwrap();
        catalog_guard.create_table(table).unwrap();
    }
    
    // Create a test row with valid types
    let mut row = Row::new();
    row.set("id".to_string(), DataValue::Integer(1));
    row.set("name".to_string(), DataValue::Text("John Doe".to_string()));
    row.set("age".to_string(), DataValue::Integer(30));
    row.set("salary".to_string(), DataValue::Float(75000.0));
    row.set("is_active".to_string(), DataValue::Boolean(true));
    
    // Validate the row
    {
        let catalog_guard = catalog.read().unwrap();
        let table = catalog_guard.get_table(&table_name).unwrap();
        let values: Vec<(&String, &DataValue)> = row.values_with_names().collect();
        
        // This should succeed
        let result = TypeValidator::validate_row(&values, &table);
        assert!(result.is_ok(), "Valid row validation failed: {}", result.err().unwrap());
    }
    
    // Create an expression for validation
    let expression = Expression::BinaryOp {
        left: Box::new(Expression::Column(ColumnReference {
            table: Some(table_name.clone()),
            name: "age".to_string(),
        })),
        op: Operator::Plus,
        right: Box::new(Expression::Literal(Value::Integer(5))),
    };
    
    // Validate the expression
    {
        let catalog_guard = catalog.read().unwrap();
        let table = catalog_guard.get_table(&table_name).unwrap();
        
        // This should return Integer type
        let result = TypeValidator::get_expression_type(&expression, &table);
        assert!(result.is_ok(), "Expression type validation failed: {}", result.err().unwrap());
        
        let expr_type = result.unwrap();
        assert_eq!(expr_type, DataType::Integer, "Expression should be Integer type");
    }
    
    // Test with invalid data type
    let mut invalid_row = Row::new();
    invalid_row.set("id".to_string(), DataValue::Integer(1));
    invalid_row.set("name".to_string(), DataValue::Integer(123)); // Name should be string but is integer
    invalid_row.set("age".to_string(), DataValue::Integer(30));
    invalid_row.set("salary".to_string(), DataValue::Float(75000.0));
    invalid_row.set("is_active".to_string(), DataValue::Boolean(true));
    
    // Validate the row (should fail)
    {
        let catalog_guard = catalog.read().unwrap();
        let table = catalog_guard.get_table(&table_name).unwrap();
        let values: Vec<(&String, &DataValue)> = invalid_row.values_with_names().collect();
        
        let result = TypeValidator::validate_row(&values, &table);
        assert!(result.is_err(), "Invalid row validation should fail");
    }
} 