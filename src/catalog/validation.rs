// Type Validation Module
//
// This module handles schema validation and type checking for the database

use crate::catalog::value;
use crate::catalog::compatibility;
use crate::catalog::expression;
use crate::catalog::convert;
use crate::catalog::schema::DataType;
use crate::catalog::column::Column;
use crate::catalog::table::Table;
use crate::query::executor::result::DataValue;
use crate::query::parser::ast::{Expression, Value, Operator};
use crate::catalog::{ValidationError, ValidationResult};

/// The type validator handles schema validation and type checking
pub struct TypeValidator;

impl TypeValidator {
    /// Validate a value against a column's data type
    pub fn validate_value(value: &DataValue, column: &Column) -> ValidationResult<()> {
        value::validate_value(value, column)
    }
    
    /// Validate all values for a row against a table schema
    pub fn validate_row(values: &[(&String, &DataValue)], table: &Table) -> ValidationResult<()> {
        value::validate_row(values, table)
    }
    
    /// Check if two data types are compatible for an operation
    pub fn check_type_compatibility(left_type: &DataType, op: &Operator, right_type: &DataType) -> ValidationResult<DataType> {
        compatibility::check_type_compatibility(left_type, op, right_type)
    }
    
    /// Determine the result type of an expression
    pub fn get_expression_type(expr: &Expression, table: &Table) -> ValidationResult<DataType> {
        expression::get_expression_type(expr, table)
    }
    
    /// Convert from parser's Value type to executor's DataValue type
    pub fn convert_value(value: &Value) -> DataValue {
        convert::convert_value(value)
    }
    
    /// Convert a value to match the target data type if possible
    pub fn convert_to_type(value: DataValue, target_type: &DataType) -> ValidationResult<DataValue> {
        convert::convert_to_type(value, target_type)
    }
} 