// Type Conversion Utilities for the Execution Engine
//
// This module provides utilities for converting between different data types.

use crate::query::executor::result::{QueryResult, QueryError, DataValue};
use crate::query::parser::ast::{Expression, Value as AstValue};
use crate::catalog::schema::DataType as CatalogDataType;

/// Convert catalog schema DataType to AST DataType
pub fn convert_catalog_dt_to_ast_dt(catalog_dt: &CatalogDataType) -> Option<crate::query::parser::ast::DataType> {
    match catalog_dt {
        CatalogDataType::Integer => Some(crate::query::parser::ast::DataType::Integer),
        CatalogDataType::Float => Some(crate::query::parser::ast::DataType::Float),
        CatalogDataType::Text => Some(crate::query::parser::ast::DataType::Text),
        CatalogDataType::Boolean => Some(crate::query::parser::ast::DataType::Boolean),
        CatalogDataType::Date => Some(crate::query::parser::ast::DataType::Date),
        CatalogDataType::Timestamp => Some(crate::query::parser::ast::DataType::Timestamp),
        // Assuming catalog doesn't have Time yet, or it maps from Text or another type.
        // If catalog::schema::DataType gets a Time variant, this should map to it.
        CatalogDataType::Blob => None, 
    }
}

/// Convert AST DataType to catalog schema DataType
pub fn helper_ast_dt_to_catalog_dt(ast_dt: &crate::query::parser::ast::DataType) -> QueryResult<crate::catalog::schema::DataType> {
    crate::catalog::schema::DataType::from_str(&ast_dt.to_string())
        .map_err(|_| QueryError::TypeError(format!("Cannot map AST type {:?} to catalog type", ast_dt)))
}

/// Cast a DataValue to a different data type if needed
pub fn cast_to_type_if_needed(value: DataValue, target_ast_type: Option<crate::query::parser::ast::DataType>) -> QueryResult<DataValue> {
    if target_ast_type.is_none() {
        return Ok(value);
    }

    match value {
        DataValue::Null => Ok(DataValue::Null), // Null casts to Null
        DataValue::Text(s) => {
            // Attempt to parse s into the target type
            if let Some(target_type) = target_ast_type {
                match target_type {
                    crate::query::parser::ast::DataType::Integer => s.parse::<i64>().map(DataValue::Integer).map_err(|e| QueryError::ExecutionError(format!("Invalid cast from text '{}' to integer: {}", s, e))),
                    crate::query::parser::ast::DataType::Float => s.parse::<f64>().map(DataValue::Float).map_err(|e| QueryError::ExecutionError(format!("Invalid cast from text '{}' to float: {}", s, e))),
                    crate::query::parser::ast::DataType::Boolean => {
                        match s.to_lowercase().as_str() {
                            "true" | "t" | "1" => Ok(DataValue::Boolean(true)),
                            "false" | "f" | "0" => Ok(DataValue::Boolean(false)),
                            _ => Err(QueryError::ExecutionError(format!("Invalid cast from text '{}' to boolean", s)))
                        }
                    },
                    crate::query::parser::ast::DataType::Date => {
                        // Basic YYYY-MM-DD check. Real parsing is more complex.
                        if s.len() == 10 && s.chars().nth(4) == Some('-') && s.chars().nth(7) == Some('-') {
                            Ok(DataValue::Date(s))
                        } else {
                            Err(QueryError::ExecutionError(format!("Invalid cast from text '{}' to date. Expected YYYY-MM-DD.", s)))
                        }
                    },
                    crate::query::parser::ast::DataType::Time => {
                         Err(QueryError::ExecutionError("Casting text to Time not yet implemented".to_string()))
                    },
                    crate::query::parser::ast::DataType::Timestamp => {
                        // Basic check. Real parsing is more complex.
                        if s.len() >= 19  {
                            Ok(DataValue::Timestamp(s))
                        } else {
                            Err(QueryError::ExecutionError(format!("Invalid cast from text '{}' to timestamp.",s)))
                        }
                    },
                    crate::query::parser::ast::DataType::Text => Ok(DataValue::Text(s)), // Text to Text is a no-op
                }
            } else {
                Ok(DataValue::Text(s)) // No target type, return as is
            }
        }
        DataValue::Integer(i) => {
             match target_ast_type {
                Some(crate::query::parser::ast::DataType::Float) => Ok(DataValue::Float(i as f64)),
                Some(crate::query::parser::ast::DataType::Text) => Ok(DataValue::Text(i.to_string())),
                Some(crate::query::parser::ast::DataType::Integer) => Ok(DataValue::Integer(i)), // No-op
                Some(crate::query::parser::ast::DataType::Boolean) => Ok(DataValue::Boolean(i != 0)), // 0 is false, others true
                Some(crate::query::parser::ast::DataType::Time) => Err(QueryError::ExecutionError("Casting Integer to Time not supported".to_string())),
                Some(_) => Err(QueryError::ExecutionError(format!("Unsupported cast from Integer to {:?}", target_ast_type.unwrap()))),
                None => Ok(DataValue::Integer(i)),
            }
        }
        DataValue::Float(f) => {
            match target_ast_type { 
                Some(crate::query::parser::ast::DataType::Integer) => Ok(DataValue::Integer(f.trunc() as i64)), // Truncate
                Some(crate::query::parser::ast::DataType::Text) => Ok(DataValue::Text(f.to_string())),
                Some(crate::query::parser::ast::DataType::Float) => Ok(DataValue::Float(f)), // No-op
                Some(crate::query::parser::ast::DataType::Time) => Err(QueryError::ExecutionError("Casting Float to Time not supported".to_string())),
                Some(_) => Err(QueryError::ExecutionError(format!("Unsupported cast from Float to {:?}", target_ast_type.unwrap()))),
                None => Ok(DataValue::Float(f)),
            }
        }
        DataValue::Boolean(b) => {
             match target_ast_type {
                Some(crate::query::parser::ast::DataType::Text) => Ok(DataValue::Text(b.to_string())),
                Some(crate::query::parser::ast::DataType::Integer) => Ok(DataValue::Integer(if b {1} else {0})),
                Some(crate::query::parser::ast::DataType::Boolean) => Ok(DataValue::Boolean(b)), // No-op
                Some(crate::query::parser::ast::DataType::Time) => Err(QueryError::ExecutionError("Casting Boolean to Time not supported".to_string())),
                Some(_) => Err(QueryError::ExecutionError(format!("Unsupported cast from Boolean to {:?}", target_ast_type.unwrap()))),
                None => Ok(DataValue::Boolean(b)),
            }
        }
        DataValue::Date(d) => { // Assuming Date is stored as String internally in DataValue
            match target_ast_type {
                Some(crate::query::parser::ast::DataType::Text) => Ok(DataValue::Text(d)),
                Some(crate::query::parser::ast::DataType::Timestamp) => Ok(DataValue::Timestamp(format!("{} 00:00:00", d))), // Basic conversion
                Some(crate::query::parser::ast::DataType::Date) => Ok(DataValue::Date(d)), // No-op
                Some(crate::query::parser::ast::DataType::Time) => Err(QueryError::ExecutionError("Casting Date to Time not supported".to_string())),
                Some(_) => Err(QueryError::ExecutionError(format!("Unsupported cast from Date to {:?}", target_ast_type.unwrap()))),
                None => Ok(DataValue::Date(d)),
            }
        }
        DataValue::Timestamp(ts) => { // Assuming Timestamp is stored as String internally
             match target_ast_type {
                Some(crate::query::parser::ast::DataType::Text) => Ok(DataValue::Text(ts)),
                Some(crate::query::parser::ast::DataType::Date) => Ok(DataValue::Date(ts.split(' ').next().unwrap_or_default().to_string())), // Basic conversion
                Some(crate::query::parser::ast::DataType::Timestamp) => Ok(DataValue::Timestamp(ts)), // No-op
                Some(crate::query::parser::ast::DataType::Time) => Err(QueryError::ExecutionError("Casting Timestamp to Time not supported".to_string())),
                Some(_) => Err(QueryError::ExecutionError(format!("Unsupported cast from Timestamp to {:?}", target_ast_type.unwrap()))),
                None => Ok(DataValue::Timestamp(ts)),
            }
        }
        DataValue::Blob(_) => { // Blob casting (e.g., to text if hex encoded, or error)
            match target_ast_type {
                // Example: cast blob to text if it's a common request, might involve hex encoding
                // Some(crate::query::parser::ast::DataType::Text) => Ok(DataValue::Text(hex::encode(data))), 
                Some(crate::query::parser::ast::DataType::Time) => Err(QueryError::ExecutionError("Casting Blob to Time not supported".to_string())),
                Some(_) => Err(QueryError::ExecutionError("Casting from Blob not supported for this type".to_string())),
                None => Ok(value), // No cast requested, return as is
            }
        }
    }
}

/// Converts an AST Value into a DataValue, attempting to use schema for type hinting strings
pub fn ast_value_to_data_value(ast_value: &AstValue, target_schema_type: Option<&CatalogDataType>) -> QueryResult<DataValue> {
    let data_value = match ast_value {
        AstValue::Integer(i) => DataValue::Integer(*i),
        AstValue::Float(f) => DataValue::Float(*f),
        AstValue::String(s) => DataValue::Text(s.clone()),
        AstValue::Boolean(b) => DataValue::Boolean(*b),
        AstValue::Null => DataValue::Null,
    };

    if let AstValue::String(_) = ast_value {
        if let Some(schema_dt) = target_schema_type {
            let target_ast_type = convert_catalog_dt_to_ast_dt(schema_dt);
            return cast_to_type_if_needed(data_value, target_ast_type);
        }
    }
    Ok(data_value)
} 