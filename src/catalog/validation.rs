// Type Validation Module
//
// This module handles schema validation and type checking for the database

use crate::catalog::schema::DataType;
use crate::catalog::column::Column;
use crate::catalog::table::Table;
use crate::query::executor::result::DataValue;
use crate::query::parser::ast::{Expression, Value, Operator};

use thiserror::Error;

/// Validation errors that can occur during type checking
#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch {
        expected: String,
        actual: String,
    },
    
    #[error("NULL value not allowed for non-nullable column {0}")]
    NullValueNotAllowed(String),
    
    #[error("Invalid operation: {0} {1} {2}")]
    InvalidOperation(String, String, String),
    
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
    
    #[error("Table not found: {0}")]
    TableNotFound(String),
    
    #[error("Incompatible types for operation: {0} {1} {2}")]
    IncompatibleTypes(String, String, String),
    
    #[error("Value out of range for type: {0}")]
    ValueOutOfRange(String),
    
    #[error("Invalid date format: {0}")]
    InvalidDateFormat(String),
    
    #[error("Invalid timestamp format: {0}")]
    InvalidTimestampFormat(String),
    
    #[error("Missing value for not null column: {0}")]
    MissingValueForNotNullColumn(String),
}

/// Type validation result
pub type ValidationResult<T> = Result<T, ValidationError>;

/// The type validator handles schema validation and type checking
pub struct TypeValidator;

impl TypeValidator {
    /// Validate a value against a column's data type
    pub fn validate_value(value: &DataValue, column: &Column) -> ValidationResult<()> {
        // Check for NULL in non-nullable columns
        if matches!(value, DataValue::Null) && !column.is_nullable() {
            return Err(ValidationError::NullValueNotAllowed(column.name().to_string()));
        }
        
        // Check type compatibility
        match (value, column.data_type()) {
            // NULL is compatible with any type when the column is nullable
            (DataValue::Null, _) => Ok(()),
            
            // Match exact types
            (DataValue::Integer(_), DataType::Integer) => Ok(()),
            (DataValue::Float(_), DataType::Float) => Ok(()),
            (DataValue::Text(_), DataType::Text) => Ok(()),
            (DataValue::Boolean(_), DataType::Boolean) => Ok(()),
            
            // Integer can be stored in a Float column
            (DataValue::Integer(_), DataType::Float) => Ok(()),
            
            // Any mismatches not explicitly allowed
            (actual, expected) => Err(ValidationError::TypeMismatch { 
                expected: format!("{:?}", expected), 
                actual: format!("{:?}", actual) 
            }),
        }
    }
    
    /// Validate all values for a row against a table schema
    pub fn validate_row(values: &[(&String, &DataValue)], table: &Table) -> ValidationResult<()> {
        for (col_name, value) in values {
            if let Some(column) = table.get_column(col_name) {
                Self::validate_value(value, &column)?;
            } else {
                return Err(ValidationError::ColumnNotFound(col_name.to_string()));
            }
        }
        
        // Check that all non-nullable columns without defaults are provided
        for column in table.columns() {
            if !column.is_nullable() && column.get_default_ast_literal().is_none() {
                let has_value = values.iter().any(|(name, _)| name == &column.name());
                if !has_value {
                    return Err(ValidationError::MissingValueForNotNullColumn(column.name().to_string()));
                }
            }
        }
        
        Ok(())
    }
    
    /// Check if two data types are compatible for an operation
    pub fn check_type_compatibility(left_type: &DataType, op: &Operator, right_type: &DataType) -> ValidationResult<DataType> {
        match op {
            // Arithmetic operations
            Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide | Operator::Modulo => {
                match (left_type, right_type) {
                    // Numeric types are compatible
                    (DataType::Integer, DataType::Integer) => Ok(DataType::Integer),
                    (DataType::Integer, DataType::Float) | (DataType::Float, DataType::Integer) | (DataType::Float, DataType::Float) => Ok(DataType::Float),
                    
                    // String concatenation for +
                    (DataType::Text, DataType::Text) if matches!(op, Operator::Plus) => Ok(DataType::Text),
                    
                    // All other combinations are invalid
                    _ => Err(ValidationError::IncompatibleTypes(
                        format!("{:?}", left_type),
                        format!("{:?}", op),
                        format!("{:?}", right_type)
                    )),
                }
            },
            
            // Comparison operations - these work on any comparable types
            Operator::Equals | Operator::NotEquals | Operator::LessThan | 
            Operator::GreaterThan | Operator::LessEquals | Operator::GreaterEquals => {
                match (left_type, right_type) {
                    // Same types can be compared
                    (left, right) if left == right => Ok(DataType::Boolean),
                    
                    // Numeric types can be compared
                    (DataType::Integer, DataType::Float) | (DataType::Float, DataType::Integer) => Ok(DataType::Boolean),
                    
                    // Different types that can't be compared
                    _ => Err(ValidationError::IncompatibleTypes(
                        format!("{:?}", left_type),
                        format!("{:?}", op),
                        format!("{:?}", right_type)
                    )),
                }
            },
            
            // Logical operations
            Operator::And | Operator::Or => {
                match (left_type, right_type) {
                    (DataType::Boolean, DataType::Boolean) => Ok(DataType::Boolean),
                    _ => Err(ValidationError::IncompatibleTypes(
                        format!("{:?}", left_type),
                        format!("{:?}", op),
                        format!("{:?}", right_type)
                    )),
                }
            },
            
            // Unary operation
            Operator::Not => {
                if left_type == &DataType::Boolean {
                    Ok(DataType::Boolean)
                } else {
                    Err(ValidationError::IncompatibleTypes(
                        format!("{:?}", left_type),
                        format!("{:?}", op),
                        String::new() // No right type for unary
                    ))
                }
            },
        }
    }
    
    /// Determine the result type of an expression
    pub fn get_expression_type(expr: &Expression, table: &Table) -> ValidationResult<DataType> {
        match expr {
            Expression::Literal(value) => {
                match value {
                    Value::Null => Err(ValidationError::TypeMismatch { 
                        expected: "non-null type".to_string(), 
                        actual: "NULL".to_string() 
                    }),
                    Value::Integer(_) => Ok(DataType::Integer),
                    Value::Float(_) => Ok(DataType::Float),
                    Value::String(_) => Ok(DataType::Text),
                    Value::Boolean(_) => Ok(DataType::Boolean),
                }
            },
            
            Expression::Column(col_ref) => {
                let column = table.get_column(&col_ref.name)
                    .ok_or_else(|| ValidationError::ColumnNotFound(col_ref.name.clone()))?;
                
                Ok(column.data_type().clone())
            },
            
            Expression::BinaryOp { left, op, right } => {
                let left_type = Self::get_expression_type(left, table)?;
                let right_type = Self::get_expression_type(right, table)?;
                
                Self::check_type_compatibility(&left_type, op, &right_type)
            },
            
            Expression::Function { name, args } => {
                // For simplicity, just hard-code a few common functions
                match name.to_uppercase().as_str() {
                    "UPPER" | "LOWER" => {
                        // These expect a string arg and return a string
                        if args.len() != 1 {
                            return Err(ValidationError::InvalidOperation(
                                name.clone(),
                                "expected 1 argument".to_string(),
                                format!("got {}", args.len())
                            ));
                        }
                        
                        let arg_type = Self::get_expression_type(&args[0], table)?;
                        if arg_type != DataType::Text {
                            return Err(ValidationError::TypeMismatch { 
                                expected: "TEXT".to_string(), 
                                actual: format!("{:?}", arg_type) 
                            });
                        }
                        
                        Ok(DataType::Text)
                    },
                    
                    "ABS" => {
                        // ABS works on numeric types
                        if args.len() != 1 {
                            return Err(ValidationError::InvalidOperation(
                                name.clone(),
                                "expected 1 argument".to_string(),
                                format!("got {}", args.len())
                            ));
                        }
                        
                        let arg_type = Self::get_expression_type(&args[0], table)?;
                        match arg_type {
                            DataType::Integer => Ok(DataType::Integer),
                            DataType::Float => Ok(DataType::Float),
                            _ => Err(ValidationError::TypeMismatch { 
                                expected: "numeric type".to_string(), 
                                actual: format!("{:?}", arg_type) 
                            }),
                        }
                    },
                    
                    // Add more functions as needed
                    
                    _ => Err(ValidationError::InvalidOperation(
                        name.clone(),
                        "unknown function".to_string(),
                        "".to_string()
                    )),
                }
            },
            
            Expression::Aggregate { function, arg } => {
                // Just return basic types for aggregate functions
                // In a real implementation, would need to validate arg type compatibility
                match function {
                    crate::query::parser::ast::AggregateFunction::Count => Ok(DataType::Integer),
                    crate::query::parser::ast::AggregateFunction::Sum => {
                        // Check the argument's type if provided
                        if let Some(arg_expr) = arg {
                            let arg_type = Self::get_expression_type(arg_expr, table)?;
                            match arg_type {
                                DataType::Integer => Ok(DataType::Integer),
                                DataType::Float => Ok(DataType::Float),
                                _ => Err(ValidationError::TypeMismatch { 
                                    expected: "numeric type".to_string(), 
                                    actual: format!("{:?}", arg_type) 
                                }),
                            }
                        } else {
                            // Without an argument, default to Integer
                            Ok(DataType::Integer)
                        }
                    },
                    crate::query::parser::ast::AggregateFunction::Avg => Ok(DataType::Float),
                    crate::query::parser::ast::AggregateFunction::Min | crate::query::parser::ast::AggregateFunction::Max => {
                        // Type depends on the argument
                        if let Some(arg_expr) = arg {
                            Self::get_expression_type(arg_expr, table)
                        } else {
                            // Without an argument, can't determine
                            Err(ValidationError::InvalidOperation(
                                format!("{:?}", function),
                                "missing argument".to_string(),
                                "".to_string()
                            ))
                        }
                    },
                }
            },
        }
    }
    
    /// Convert from parser's Value type to executor's DataValue type
    pub fn convert_value(value: &Value) -> DataValue {
        match value {
            Value::Null => DataValue::Null,
            Value::Integer(i) => DataValue::Integer(*i),
            Value::Float(f) => DataValue::Float(*f),
            Value::String(s) => DataValue::Text(s.clone()),
            Value::Boolean(b) => DataValue::Boolean(*b),
        }
    }
    
    /// Convert a value to match the target data type if possible
    pub fn convert_to_type(value: DataValue, target_type: &DataType) -> ValidationResult<DataValue> {
        match (value.clone(), target_type) {
            // Already matches the target type
            (DataValue::Integer(_), DataType::Integer) |
            (DataValue::Float(_), DataType::Float) |
            (DataValue::Text(_), DataType::Text) |
            (DataValue::Boolean(_), DataType::Boolean) |
            (DataValue::Null, _) => Ok(value),
            
            // Type conversions
            (DataValue::Integer(i), DataType::Float) => Ok(DataValue::Float(i as f64)),
            (DataValue::Integer(i), DataType::Text) => Ok(DataValue::Text(i.to_string())),
            (DataValue::Integer(i), DataType::Boolean) => Ok(DataValue::Boolean(i != 0)),
            
            (DataValue::Float(f), DataType::Integer) => {
                // Check if the float can be accurately represented as an integer
                if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                    Ok(DataValue::Integer(f as i64))
                } else {
                    Err(ValidationError::ValueOutOfRange(format!("{} cannot be accurately converted to INTEGER", f)))
                }
            },
            (DataValue::Float(f), DataType::Text) => Ok(DataValue::Text(f.to_string())),
            (DataValue::Float(f), DataType::Boolean) => Ok(DataValue::Boolean(f != 0.0)),
            
            (DataValue::Text(s), DataType::Integer) => {
                match s.parse::<i64>() {
                    Ok(i) => Ok(DataValue::Integer(i)),
                    Err(_) => Err(ValidationError::TypeMismatch { 
                        expected: "INTEGER".to_string(), 
                        actual: format!("TEXT '{}'", s) 
                    }),
                }
            },
            (DataValue::Text(s), DataType::Float) => {
                match s.parse::<f64>() {
                    Ok(f) => Ok(DataValue::Float(f)),
                    Err(_) => Err(ValidationError::TypeMismatch { 
                        expected: "FLOAT".to_string(), 
                        actual: format!("TEXT '{}'", s) 
                    }),
                }
            },
            (DataValue::Text(s), DataType::Boolean) => {
                match s.to_lowercase().as_str() {
                    "true" | "t" | "yes" | "y" | "1" => Ok(DataValue::Boolean(true)),
                    "false" | "f" | "no" | "n" | "0" => Ok(DataValue::Boolean(false)),
                    _ => Err(ValidationError::TypeMismatch { 
                        expected: "BOOLEAN".to_string(), 
                        actual: format!("TEXT '{}'", s) 
                    }),
                }
            },
            (DataValue::Text(s), DataType::Date) => {
                // Simple format validation
                // In a real implementation, would parse and validate date format
                if s.matches('-').count() == 2 {
                    Ok(value)
                } else {
                    Err(ValidationError::InvalidDateFormat(s))
                }
            },
            (DataValue::Text(s), DataType::Timestamp) => {
                // Simple format validation
                // In a real implementation, would parse and validate timestamp format
                if s.contains('-') && s.contains(':') {
                    Ok(value)
                } else {
                    Err(ValidationError::InvalidTimestampFormat(s))
                }
            },
            
            (DataValue::Boolean(b), DataType::Integer) => Ok(DataValue::Integer(if b { 1 } else { 0 })),
            (DataValue::Boolean(b), DataType::Float) => Ok(DataValue::Float(if b { 1.0 } else { 0.0 })),
            (DataValue::Boolean(b), DataType::Text) => Ok(DataValue::Text(b.to_string())),
            
            // Types that can't be converted
            (actual, expected) => Err(ValidationError::TypeMismatch { 
                expected: format!("{:?}", expected), 
                actual: format!("{:?}", actual) 
            }),
        }
    }
}

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
        assert!(TypeValidator::validate_value(&value, &col).is_ok());
        
        // Test invalid value - null
        let value = DataValue::Null;
        assert!(TypeValidator::validate_value(&value, &col).is_err());
        
        // Test invalid value - wrong type
        let value = DataValue::Text("not an integer".to_string());
        assert!(TypeValidator::validate_value(&value, &col).is_err());
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
        assert!(TypeValidator::validate_row(&values, &table).is_ok());
        
        // Create an invalid row (missing required column)
        let mut row2 = Row::new();
        row2.set("id".to_string(), DataValue::Integer(2));
        // missing name (required)
        row2.set("active".to_string(), DataValue::Boolean(false));
        
        let values2: Vec<(&String, &DataValue)> = row2.values_with_names().collect();
        
        // Test invalid row
        assert!(TypeValidator::validate_row(&values2, &table).is_err());
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
            TypeValidator::get_expression_type(&expr, &table).unwrap(),
            DataType::Integer
        );
        
        // Test column reference
        let expr = Expression::Column(ColumnReference {
            table: None,
            name: "id".to_string(),
        });
        assert_eq!(
            TypeValidator::get_expression_type(&expr, &table).unwrap(),
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
            TypeValidator::get_expression_type(&expr, &table).unwrap(),
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
            TypeValidator::get_expression_type(&expr, &table).unwrap(),
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
        assert!(TypeValidator::get_expression_type(&expr, &table).is_err());
    }
} 