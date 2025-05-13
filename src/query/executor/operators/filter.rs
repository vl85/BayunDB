// Filter Operator
//
// Filters rows from the input operator based on a predicate.

use std::sync::{Arc, Mutex};
use crate::query::executor::result::{Row, QueryResult, DataValue, QueryError};
use crate::query::executor::operators::Operator;
use crate::query::parser::ast::Expression;
use crate::query::executor::expression_eval;

/// Filter operator
pub struct FilterOperator {
    /// Input operator
    input: Arc<Mutex<dyn Operator + Send + Sync>>,
    /// Predicate expression
    predicate: Expression,
}

impl FilterOperator {
    /// Create a new filter operator
    pub fn new(input: Arc<Mutex<dyn Operator + Send + Sync>>, predicate: Expression) -> Self {
        FilterOperator { input, predicate }
    }
    
    /// Evaluate an expression against a row
    fn evaluate_expression(&self, expr: &Expression, row: &Row) -> QueryResult<DataValue> {
        // Delegate all expression evaluation to the global helper.
        // The FilterOperator itself doesn't need schema context beyond what the input row provides.
        expression_eval::evaluate_expression(expr, row, None)
    }
}

impl Operator for FilterOperator {
    fn init(&mut self) -> QueryResult<()> {
        self.input.lock().unwrap().init()
    }
    
    fn next(&mut self) -> QueryResult<Option<Row>> {
        loop {
            let next_row_option = self.input.lock().unwrap().next()?;
            match next_row_option {
                Some(row) => {
                    // Evaluate the predicate
                    let result = expression_eval::evaluate_expression(&self.predicate, &row, None)?;
                    
                    // Check if the result is boolean
                    match result {
                        DataValue::Boolean(b) => {
                            if b {
                                return Ok(Some(row)); // Row passes filter
                            }
                            // else, boolean is false, continue to next row
                        }
                        DataValue::Null => {
                            // Treat NULL as false in filter predicates, continue loop
                        }
                        _ => {
                            // Predicate evaluated to a non-boolean, non-null type. This is an error.
                            return Err(QueryError::TypeError(format!(
                                "Filter predicate evaluated to non-boolean type: {:?}",
                                result.get_type()
                            )));
                        }
                    }
                }
                None => return Ok(None), // End of input
            }
        }
    }
    
    fn close(&mut self) -> QueryResult<()> {
        self.input.lock().unwrap().close()
    }
}

// Renamed from DummyOperator to avoid conflict with operators::dummy::DummyOperator
pub struct FilterTestInputOperator {
    rows: Vec<Row>,
    index: usize,
    initialized: bool,
}

impl FilterTestInputOperator {
    pub fn new(rows: Vec<Row>) -> Self {
        FilterTestInputOperator {
            rows,
            index: 0,
            initialized: false,
        }
    }
}

impl Operator for FilterTestInputOperator {
    fn init(&mut self) -> QueryResult<()> {
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
        self.index = 0; // Reset for potential reuse?
        self.initialized = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::query::parser::ast::{ColumnReference, Expression, Operator as AstOperator, Value};
    use std::sync::{Arc, Mutex};
    use crate::query::executor::result::{DataValue, QueryError, Row};
    use super::{FilterOperator, FilterTestInputOperator}; // Assuming FilterTestInputOperator is in filter.rs
    use crate::query::executor::operators::Operator; // Import the Operator trait

    #[test]
    fn test_filter_operator_simple() {
        // Setup dummy input operator
        let rows = vec![
            Row::from_values(vec!["a".to_string()], vec![DataValue::Integer(1)]),
            Row::from_values(vec!["a".to_string()], vec![DataValue::Integer(2)]),
            Row::from_values(vec!["a".to_string()], vec![DataValue::Integer(3)]),
        ];
        let dummy = Arc::new(Mutex::new(FilterTestInputOperator::new(rows))); // Use renamed struct

        // Predicate: a = 2
        let predicate = Expression::BinaryOp {
            left: Box::new(Expression::Column(ColumnReference { table: None, name: "a".to_string() })),
            op: AstOperator::Equals,
            right: Box::new(Expression::Literal(Value::Integer(2))),
        };
        let mut filter_op = FilterOperator::new(dummy, predicate);

        filter_op.init().unwrap();
        let row1 = filter_op.next().unwrap();
        assert!(row1.is_some());
        assert_eq!(row1.unwrap().get("a"), Some(&DataValue::Integer(2)));
        assert!(filter_op.next().unwrap().is_none());
        filter_op.close().unwrap();
    }

    #[test]
    fn test_filter_operator_no_match() {
         let rows = vec![
            Row::from_values(vec!["a".to_string()], vec![DataValue::Integer(1)]),
        ];
        let dummy = Arc::new(Mutex::new(FilterTestInputOperator::new(rows))); // Use renamed struct

        // Predicate: a = 5
        let predicate = Expression::BinaryOp {
            left: Box::new(Expression::Column(ColumnReference { table: None, name: "a".to_string() })),
            op: AstOperator::Equals,
            right: Box::new(Expression::Literal(Value::Integer(5))), // Should not match
        };
        let mut filter_op = FilterOperator::new(dummy, predicate);

        filter_op.init().unwrap();
        assert!(filter_op.next().unwrap().is_none());
        filter_op.close().unwrap();
    }

    #[test]
    fn test_filter_operator_error() {
        // Predicate returns non-boolean error during eval (implicitly)
        let rows = vec![
            Row::from_values(vec!["a".to_string()], vec![DataValue::Integer(1)]),
        ];
        let dummy = Arc::new(Mutex::new(FilterTestInputOperator::new(rows))); // Use renamed struct
        
        let predicate = Expression::BinaryOp {
            left: Box::new(Expression::Column(ColumnReference { table: None, name: "a".to_string() })),
            op: AstOperator::Plus,
            right: Box::new(Expression::Literal(Value::Integer(1))),
        };
        let mut filter_op = FilterOperator::new(dummy, predicate);

        filter_op.init().unwrap();
        let result = filter_op.next();
        assert!(result.is_err());
        match result.err().unwrap() {
            QueryError::TypeError(_) => { /* Expected */ },
            e => panic!("Expected TypeError, got {:?}", e),
        }
        filter_op.close().unwrap();
    }
}

/// Factory function for FilterOperator
pub fn create_filter(
    input: Arc<Mutex<dyn Operator + Send + Sync>>,
    predicate: Expression,
) -> QueryResult<Arc<Mutex<dyn Operator + Send + Sync>>> {
    Ok(Arc::new(Mutex::new(FilterOperator::new(input, predicate))))
} 