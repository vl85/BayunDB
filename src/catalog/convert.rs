use crate::query::parser::ast::Value;
use crate::query::executor::result::DataValue;
use crate::catalog::schema::DataType;
use crate::catalog::{ValidationError, ValidationResult};

pub fn convert_value(value: &Value) -> DataValue {
    match value {
        Value::Null => DataValue::Null,
        Value::Integer(i) => DataValue::Integer(*i),
        Value::Float(f) => DataValue::Float(*f),
        Value::String(s) => DataValue::Text(s.clone()),
        Value::Boolean(b) => DataValue::Boolean(*b),
    }
}

pub fn convert_to_type(value: DataValue, target_type: &DataType) -> ValidationResult<DataValue> {
    match (value.clone(), target_type) {
        (DataValue::Integer(_), DataType::Integer) |
        (DataValue::Float(_), DataType::Float) |
        (DataValue::Text(_), DataType::Text) |
        (DataValue::Boolean(_), DataType::Boolean) |
        (DataValue::Null, _) => Ok(value),
        (DataValue::Integer(i), DataType::Float) => Ok(DataValue::Float(i as f64)),
        (DataValue::Integer(i), DataType::Text) => Ok(DataValue::Text(i.to_string())),
        (DataValue::Integer(i), DataType::Boolean) => Ok(DataValue::Boolean(i != 0)),
        (DataValue::Float(f), DataType::Integer) => {
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
            if s.matches('-').count() == 2 {
                Ok(value)
            } else {
                Err(ValidationError::InvalidDateFormat(s))
            }
        },
        (DataValue::Text(s), DataType::Timestamp) => {
            if s.contains('-') && s.contains(':') {
                Ok(value)
            } else {
                Err(ValidationError::InvalidTimestampFormat(s))
            }
        },
        (DataValue::Boolean(b), DataType::Integer) => Ok(DataValue::Integer(if b { 1 } else { 0 })),
        (DataValue::Boolean(b), DataType::Float) => Ok(DataValue::Float(if b { 1.0 } else { 0.0 })),
        (DataValue::Boolean(b), DataType::Text) => Ok(DataValue::Text(b.to_string())),
        (actual, expected) => Err(ValidationError::TypeMismatch { 
            expected: format!("{:?}", expected), 
            actual: format!("{:?}", actual) 
        }),
    }
} 