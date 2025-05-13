use crate::catalog::column::Column;
use crate::catalog::table::Table;
use crate::query::executor::result::DataValue;
use crate::catalog::{ValidationError, ValidationResult};

pub fn validate_value(value: &DataValue, column: &Column) -> ValidationResult<()> {
    if matches!(value, DataValue::Null) && !column.is_nullable() {
        return Err(ValidationError::NullValueNotAllowed(column.name().to_string()));
    }
    match (value, column.data_type()) {
        (DataValue::Null, _) => Ok(()),
        (DataValue::Integer(_), crate::catalog::schema::DataType::Integer) => Ok(()),
        (DataValue::Float(_), crate::catalog::schema::DataType::Float) => Ok(()),
        (DataValue::Text(_), crate::catalog::schema::DataType::Text) => Ok(()),
        (DataValue::Boolean(_), crate::catalog::schema::DataType::Boolean) => Ok(()),
        (DataValue::Integer(_), crate::catalog::schema::DataType::Float) => Ok(()),
        (actual, expected) => Err(ValidationError::TypeMismatch { 
            expected: format!("{:?}", expected), 
            actual: format!("{:?}", actual) 
        }),
    }
}

pub fn validate_row(values: &[(&String, &DataValue)], table: &Table) -> ValidationResult<()> {
    for (col_name, value) in values {
        if let Some(column) = table.get_column(col_name) {
            validate_value(value, column)?;
        } else {
            return Err(ValidationError::ColumnNotFound(col_name.to_string()));
        }
    }
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