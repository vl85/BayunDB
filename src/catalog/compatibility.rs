use crate::catalog::schema::DataType;
use crate::query::parser::ast::Operator;
use crate::catalog::{ValidationError, ValidationResult};

pub fn check_type_compatibility(left_type: &DataType, op: &Operator, right_type: &DataType) -> ValidationResult<DataType> {
    match op {
        Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide | Operator::Modulo => {
            match (left_type, right_type) {
                (DataType::Integer, DataType::Integer) => Ok(DataType::Integer),
                (DataType::Integer, DataType::Float) | (DataType::Float, DataType::Integer) | (DataType::Float, DataType::Float) => Ok(DataType::Float),
                (DataType::Text, DataType::Text) if matches!(op, Operator::Plus) => Ok(DataType::Text),
                _ => Err(ValidationError::IncompatibleTypes(
                    format!("{:?}", left_type),
                    format!("{:?}", op),
                    format!("{:?}", right_type)
                )),
            }
        },
        Operator::Equals | Operator::NotEquals | Operator::LessThan | 
        Operator::GreaterThan | Operator::LessEquals | Operator::GreaterEquals => {
            match (left_type, right_type) {
                (left, right) if left == right => Ok(DataType::Boolean),
                (DataType::Integer, DataType::Float) | (DataType::Float, DataType::Integer) => Ok(DataType::Boolean),
                _ => Err(ValidationError::IncompatibleTypes(
                    format!("{:?}", left_type),
                    format!("{:?}", op),
                    format!("{:?}", right_type)
                )),
            }
        },
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
        Operator::Not => {
            if left_type == &DataType::Boolean {
                Ok(DataType::Boolean)
            } else {
                Err(ValidationError::IncompatibleTypes(
                    format!("{:?}", left_type),
                    format!("{:?}", op),
                    String::new()
                ))
            }
        },
    }
} 