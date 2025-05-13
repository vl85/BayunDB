#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::table::Table;
    use crate::catalog::column::Column;
    use crate::catalog::schema::DataType;
    use crate::query::executor::result::{DataValue, Row};
    use crate::query::parser::ast::{Expression, Value, Operator, ColumnReference};

    #[test]
    fn test_value_validation() {
        // Create a test column
        let col = Column::new(
            "id".to_string(),
            DataType::Integer,
            false, // not nullable
            true,  // primary key
            None,  // no default
        );
        
        // Test valid value
        let value = DataValue::Integer(42);
        assert!(crate::catalog::TypeValidator::validate_value(&value, &col).is_ok());
        
        // Test invalid value - null
        let value = DataValue::Null;
        assert!(crate::catalog::TypeValidator::validate_value(&value, &col).is_err());
        
        // Test invalid value - wrong type
        let value = DataValue::Text("not an integer".to_string());
        assert!(crate::catalog::TypeValidator::validate_value(&value, &col).is_err());
    }
    
    #[test]
    fn test_row_validation() {
        // Create a test table with columns
        let columns = vec![
            Column::new("id".to_string(), DataType::Integer, false, true, None),
            Column::new("name".to_string(), DataType::Text, false, false, None),
            Column::new("active".to_string(), DataType::Boolean, true, false, None),
        ];
        
        let table = Table::new("test_table".to_string(), columns);
        
        // Create a valid row
        let mut row = Row::new();
        row.set("id".to_string(), DataValue::Integer(1));
        row.set("name".to_string(), DataValue::Text("Test".to_string()));
        row.set("active".to_string(), DataValue::Boolean(true));
        
        // Get values with names for validation
        let values: Vec<(&String, &DataValue)> = row.values_with_names().collect();
        
        // Test valid row
        assert!(crate::catalog::TypeValidator::validate_row(&values, &table).is_ok());
        
        // Create an invalid row (missing required column)
        let mut row2 = Row::new();
        row2.set("id".to_string(), DataValue::Integer(2));
        // missing name (required)
        row2.set("active".to_string(), DataValue::Boolean(false));
        
        let values2: Vec<(&String, &DataValue)> = row2.values_with_names().collect();
        
        // Test invalid row
        assert!(crate::catalog::TypeValidator::validate_row(&values2, &table).is_err());
    }
    
    #[test]
    fn test_expression_type() {
        // Create a test table with columns
        let columns = vec![
            Column::new("id".to_string(), DataType::Integer, false, true, None),
            Column::new("name".to_string(), DataType::Text, false, false, None),
            Column::new("value".to_string(), DataType::Float, true, false, None),
            Column::new("active".to_string(), DataType::Boolean, true, false, None),
        ];
        
        let table = Table::new("test_table".to_string(), columns);
        
        // Test literal expression
        let expr = Expression::Literal(Value::Integer(42));
        assert_eq!(
            crate::catalog::TypeValidator::get_expression_type(&expr, &table).unwrap(),
            DataType::Integer
        );
        
        // Test column reference
        let expr = Expression::Column(ColumnReference {
            table: None,
            name: "id".to_string(),
        });
        assert_eq!(
            crate::catalog::TypeValidator::get_expression_type(&expr, &table).unwrap(),
            DataType::Integer
        );
        
        // Test binary operation - addition
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column(ColumnReference {
                table: None,
                name: "id".to_string(),
            })),
            op: Operator::Plus,
            right: Box::new(Expression::Literal(Value::Integer(5))),
        };
        assert_eq!(
            crate::catalog::TypeValidator::get_expression_type(&expr, &table).unwrap(),
            DataType::Integer
        );
        
        // Test binary operation - comparison
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column(ColumnReference {
                table: None,
                name: "id".to_string(),
            })),
            op: Operator::Equals,
            right: Box::new(Expression::Literal(Value::Integer(5))),
        };
        assert_eq!(
            crate::catalog::TypeValidator::get_expression_type(&expr, &table).unwrap(),
            DataType::Boolean
        );
        
        // Test invalid binary operation
        let expr = Expression::BinaryOp {
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
        assert!(crate::catalog::TypeValidator::get_expression_type(&expr, &table).is_err());
    }
} 