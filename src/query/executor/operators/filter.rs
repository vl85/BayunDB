// Filter Operator
//
// Filters rows from the input operator based on a predicate.

use std::sync::{Arc, Mutex};
use crate::query::executor::result::{Row, QueryResult, DataValue, QueryError};
use crate::query::executor::operators::Operator;
use crate::catalog::TypeValidator;
use crate::query::parser::ast::{Expression, Operator as AstOperator, Value};

/// Filter operator
pub struct FilterOperator {
    /// Input operator
    input: Arc<Mutex<dyn Operator + Send>>,
    /// Predicate expression
    expression: Expression,
    /// Table name for column resolution
    table_name: String,
    /// Whether the operator is initialized
    initialized: bool,
}

impl FilterOperator {
    /// Create a new filter operator
    pub fn new(
        input: Arc<Mutex<dyn Operator + Send>>,
        expression: Expression,
        table_name: String,
    ) -> Self {
        FilterOperator {
            input,
            expression,
            table_name,
            initialized: false,
        }
    }
    
    /// Evaluate an expression against a row
    fn evaluate_expression(&self, expr: &Expression, row: &Row) -> QueryResult<DataValue> {
        match expr {
            Expression::Literal(value) => {
                let data_value = TypeValidator::convert_value(value);
                Ok(data_value)
            },
            
            Expression::Column(col_ref) => {
                // Construct the name to look up in the row.
                // If col_ref.table is Some (e.g., "t1.id" in predicate), use "t1.id".
                // If col_ref.table is None (e.g., "id" in predicate), use "self.table_name.id" 
                // (where self.table_name is the alias of the FilterOperator's input).
                let lookup_name = match &col_ref.table {
                    Some(table_qualifier) => format!("{}.{}", table_qualifier, col_ref.name),
                    None => {
                        // Predicate has unqualified column, e.g., "id > 5"
                        // Assume it refers to the input table/alias of this filter operator.
                        // TableScanOperator produces aliased names like "alias.id".
                        format!("{}.{}", self.table_name, col_ref.name)
                    }
                };

                let value = row.get(&lookup_name)
                    .ok_or_else(|| QueryError::ColumnNotFound(lookup_name.clone()))?;
                
                Ok(value.clone())
            },
            
            Expression::BinaryOp { left, op, right } => {
                let left_value = self.evaluate_expression(left, row)?;
                let right_value = self.evaluate_expression(right, row)?;
                
                match op {
                    // Comparison operators
                    AstOperator::Equals => Ok(DataValue::Boolean(left_value == right_value)),
                    AstOperator::NotEquals => Ok(DataValue::Boolean(left_value != right_value)),
                    
                    AstOperator::LessThan => {
                        match left_value.partial_cmp(&right_value) {
                            Some(std::cmp::Ordering::Less) => Ok(DataValue::Boolean(true)),
                            Some(_) => Ok(DataValue::Boolean(false)),
                            None => Err(QueryError::ExecutionError(format!(
                                "Cannot compare {:?} < {:?}", left_value, right_value
                            ))),
                        }
                    },
                    
                    AstOperator::GreaterThan => {
                        match left_value.partial_cmp(&right_value) {
                            Some(std::cmp::Ordering::Greater) => Ok(DataValue::Boolean(true)),
                            Some(_) => Ok(DataValue::Boolean(false)),
                            None => Err(QueryError::ExecutionError(format!(
                                "Cannot compare {:?} > {:?}", left_value, right_value
                            ))),
                        }
                    },
                    
                    AstOperator::LessEquals => {
                        match left_value.partial_cmp(&right_value) {
                            Some(ord) => Ok(DataValue::Boolean(ord != std::cmp::Ordering::Greater)),
                            None => Err(QueryError::ExecutionError(format!(
                                "Cannot compare {:?} <= {:?}", left_value, right_value
                            ))),
                        }
                    },
                    
                    AstOperator::GreaterEquals => {
                        match left_value.partial_cmp(&right_value) {
                            Some(ord) => Ok(DataValue::Boolean(ord != std::cmp::Ordering::Less)),
                            None => Err(QueryError::ExecutionError(format!(
                                "Cannot compare {:?} >= {:?}", left_value, right_value
                            ))),
                        }
                    },
                    
                    // Logical operators
                    AstOperator::And => {
                        match (left_value, right_value) {
                            (DataValue::Boolean(l), DataValue::Boolean(r)) => Ok(DataValue::Boolean(l && r)),
                            _ => Err(QueryError::ExecutionError("AND requires boolean operands".to_string())),
                        }
                    },
                    
                    AstOperator::Or => {
                        match (left_value, right_value) {
                            (DataValue::Boolean(l), DataValue::Boolean(r)) => Ok(DataValue::Boolean(l || r)),
                            _ => Err(QueryError::ExecutionError("OR requires boolean operands".to_string())),
                        }
                    },
                    
                    // Arithmetic operators
                    AstOperator::Plus => {
                        match (&left_value, &right_value) {
                            (DataValue::Integer(l), DataValue::Integer(r)) => Ok(DataValue::Integer(l + r)),
                            (DataValue::Integer(l), DataValue::Float(r)) => Ok(DataValue::Float(*l as f64 + r)),
                            (DataValue::Float(l), DataValue::Integer(r)) => Ok(DataValue::Float(l + *r as f64)),
                            (DataValue::Float(l), DataValue::Float(r)) => Ok(DataValue::Float(l + r)),
                            (DataValue::Text(l), DataValue::Text(r)) => Ok(DataValue::Text(l.clone() + r)),
                            _ => Err(QueryError::ExecutionError(format!(
                                "Cannot add {:?} and {:?}", left_value, right_value
                            ))),
                        }
                    },
                    
                    AstOperator::Minus => {
                        match (&left_value, &right_value) {
                            (DataValue::Integer(l), DataValue::Integer(r)) => Ok(DataValue::Integer(l - r)),
                            (DataValue::Integer(l), DataValue::Float(r)) => Ok(DataValue::Float(*l as f64 - r)),
                            (DataValue::Float(l), DataValue::Integer(r)) => Ok(DataValue::Float(l - *r as f64)),
                            (DataValue::Float(l), DataValue::Float(r)) => Ok(DataValue::Float(l - r)),
                            _ => Err(QueryError::ExecutionError(format!(
                                "Cannot subtract {:?} from {:?}", right_value, left_value
                            ))),
                        }
                    },
                    
                    AstOperator::Multiply => {
                        match (&left_value, &right_value) {
                            (DataValue::Integer(l), DataValue::Integer(r)) => Ok(DataValue::Integer(l * r)),
                            (DataValue::Integer(l), DataValue::Float(r)) => Ok(DataValue::Float(*l as f64 * r)),
                            (DataValue::Float(l), DataValue::Integer(r)) => Ok(DataValue::Float(l * *r as f64)),
                            (DataValue::Float(l), DataValue::Float(r)) => Ok(DataValue::Float(l * r)),
                            _ => Err(QueryError::ExecutionError(format!(
                                "Cannot multiply {:?} by {:?}", left_value, right_value
                            ))),
                        }
                    },
                    
                    AstOperator::Divide => {
                        match (&left_value, &right_value) {
                            (_, DataValue::Integer(r)) if *r == 0 => {
                                Err(QueryError::ExecutionError("Division by zero".to_string()))
                            },
                            (_, DataValue::Float(r)) if *r == 0.0 => {
                                Err(QueryError::ExecutionError("Division by zero".to_string()))
                            },
                            (DataValue::Integer(l), DataValue::Integer(r)) => Ok(DataValue::Float(*l as f64 / *r as f64)),
                            (DataValue::Integer(l), DataValue::Float(r)) => Ok(DataValue::Float(*l as f64 / r)),
                            (DataValue::Float(l), DataValue::Integer(r)) => Ok(DataValue::Float(l / *r as f64)),
                            (DataValue::Float(l), DataValue::Float(r)) => Ok(DataValue::Float(l / r)),
                            _ => Err(QueryError::ExecutionError(format!(
                                "Cannot divide {:?} by {:?}", left_value, right_value
                            ))),
                        }
                    },
                    
                    AstOperator::Modulo => {
                        match (&left_value, &right_value) {
                            (_, DataValue::Integer(r)) if *r == 0 => {
                                Err(QueryError::ExecutionError("Modulo by zero".to_string()))
                            },
                            (_, DataValue::Float(r)) if *r == 0.0 => {
                                Err(QueryError::ExecutionError("Modulo by zero".to_string()))
                            },
                            (DataValue::Integer(l), DataValue::Integer(r)) => Ok(DataValue::Integer(l % r)),
                            (DataValue::Integer(l), DataValue::Float(r)) => Ok(DataValue::Float(*l as f64 % r)),
                            (DataValue::Float(l), DataValue::Integer(r)) => Ok(DataValue::Float(l % *r as f64)),
                            (DataValue::Float(l), DataValue::Float(r)) => Ok(DataValue::Float(l % r)),
                            _ => Err(QueryError::ExecutionError(format!(
                                "Cannot compute modulo of {:?} by {:?}", left_value, right_value
                            ))),
                        }
                    },
                    
                    // Unary operations would be handled differently
                    AstOperator::Not => {
                        Err(QueryError::ExecutionError("NOT operator should be handled separately".to_string()))
                    },
                }
            },
            
            // Add other expression types as needed
            _ => Err(QueryError::ExecutionError(format!("Unsupported expression: {:?}", expr))),
        }
    }
}

impl Operator for FilterOperator {
    fn init(&mut self) -> QueryResult<()> {
        // Initialize the input operator
        let mut input = self.input.lock().map_err(|e| {
            QueryError::ExecutionError(format!("Failed to lock input operator: {}", e))
        })?;
        
        input.init()?;
        self.initialized = true;
        Ok(())
    }
    
    fn next(&mut self) -> QueryResult<Option<Row>> {
        if !self.initialized {
            return Err(QueryError::ExecutionError("Operator not initialized".to_string()));
        }
        
        // Find the next row that satisfies the predicate
        loop {
            let mut input = self.input.lock().map_err(|e| {
                QueryError::ExecutionError(format!("Failed to lock input operator: {}", e))
            })?;
            
            let row_option = input.next()?;
            std::mem::drop(input);
            
            match row_option {
                Some(row) => {
                    // Evaluate predicate expression for the row
                    let result = self.evaluate_expression(&self.expression, &row)?;
                    
                    // Check if the result is a boolean true
                    match result {
                        DataValue::Boolean(true) => return Ok(Some(row)),
                        DataValue::Boolean(false) => continue, // Skip this row
                        _ => return Err(QueryError::ExecutionError(
                            format!("Predicate evaluation must return a boolean, got: {:?}", result)
                        )),
                    }
                },
                None => return Ok(None), // No more rows
            }
        }
    }
    
    fn close(&mut self) -> QueryResult<()> {
        self.initialized = false;
        let mut input = self.input.lock().map_err(|e| {
            QueryError::ExecutionError(format!("Failed to lock input operator: {}", e))
        })?;
        
        input.close()
    }
}

// Implement a DummyOperator for tests
#[cfg(test)]
pub struct DummyOperator {
    rows: Vec<Row>,
    index: usize,
    initialized: bool,
}

#[cfg(test)]
impl DummyOperator {
    pub fn new(rows: Vec<Row>) -> Self {
        DummyOperator {
            rows,
            index: 0,
            initialized: false,
        }
    }
}

#[cfg(test)]
impl Operator for DummyOperator {
    fn init(&mut self) -> QueryResult<()> {
        self.index = 0;
        self.initialized = true;
        Ok(())
    }
    
    fn next(&mut self) -> QueryResult<Option<Row>> {
        if !self.initialized {
            return Err(QueryError::ExecutionError("Operator not initialized".to_string()));
        }
        
        if self.index < self.rows.len() {
            let row = self.rows[self.index].clone();
            self.index += 1;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }
    
    fn close(&mut self) -> QueryResult<()> {
        self.initialized = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::ast::{ColumnReference, Operator as AstOperator};
    
    #[test]
    fn test_filter_operation() {
        // Create test data
        let mut rows = Vec::new();
        let table_alias = "test_table"; // The alias used when creating FilterOperator

        let mut row1 = Row::new();
        row1.set(format!("{}.id", table_alias), DataValue::Integer(1));
        row1.set(format!("{}.value", table_alias), DataValue::Integer(10));
        rows.push(row1);
        
        let mut row2 = Row::new();
        row2.set(format!("{}.id", table_alias), DataValue::Integer(2));
        row2.set(format!("{}.value", table_alias), DataValue::Integer(20));
        rows.push(row2);
        
        let mut row3 = Row::new();
        row3.set(format!("{}.id", table_alias), DataValue::Integer(3));
        row3.set(format!("{}.value", table_alias), DataValue::Integer(30));
        rows.push(row3);
        
        // Create dummy operator that will return our test data
        let dummy = Arc::new(Mutex::new(DummyOperator::new(rows)));
        
        // Create a filter expression: value > 15 (unqualified column name)
        let expression = Expression::BinaryOp {
            left: Box::new(Expression::Column(ColumnReference {
                table: None, // Unqualified, will use FilterOperator's table_name (alias)
                name: "value".to_string(),
            })),
            op: AstOperator::GreaterThan,
            right: Box::new(Expression::Literal(Value::Integer(15))),
        };
        
        // Create filter operator, providing the alias of the input
        let mut filter = FilterOperator::new(dummy, expression, table_alias.to_string());
        
        // Initialize the filter
        filter.init().unwrap();
        
        // Get the first row (should be row2)
        let result = filter.next().unwrap().unwrap();
        assert_eq!(result.get(&format!("{}.id", table_alias)), Some(&DataValue::Integer(2)));
        
        // Get the second row (should be row3)
        let result = filter.next().unwrap().unwrap();
        assert_eq!(result.get(&format!("{}.id", table_alias)), Some(&DataValue::Integer(3)));
        
        // There should be no more rows
        assert!(filter.next().unwrap().is_none());
    }
    
    #[test]
    fn test_type_compatibility() {
        // Create test data
        let mut rows = Vec::new();
        let table_alias = "test_table";

        let mut row1 = Row::new();
        row1.set(format!("{}.id", table_alias), DataValue::Integer(1));
        row1.set(format!("{}.name", table_alias), DataValue::Text("Test".to_string()));
        rows.push(row1);
        
        // Create dummy operator that will return our test data
        let dummy = Arc::new(Mutex::new(DummyOperator::new(rows)));
        
        // Create an invalid filter expression: id + name
        let expression = Expression::BinaryOp {
            left: Box::new(Expression::Column(ColumnReference {
                table: None, // Unqualified
                name: "id".to_string(),
            })),
            op: AstOperator::Plus,
            right: Box::new(Expression::Column(ColumnReference {
                table: None, // Unqualified
                name: "name".to_string(),
            })),
        };
        
        // Create filter operator
        let mut filter = FilterOperator::new(dummy, expression, table_alias.to_string());
        
        // Initialize the filter
        filter.init().unwrap();
        
        // This should fail with a type error
        assert!(filter.next().is_err());
    }
} 