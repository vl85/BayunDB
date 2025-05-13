// Projection Operator Implementation
//
// This module implements the projection operator for selecting specific columns.

use std::sync::{Arc, Mutex};
// std::io::Write; // Removed

use crate::query::executor::operators::Operator;
use crate::query::executor::result::{Row, QueryResult};
use crate::query::parser::ast::Expression;
use linked_hash_map::LinkedHashMap; // For preserving column order
use crate::query::executor::expression_eval;

/// Projection operator that selects specific columns from input rows
pub struct ProjectionOperator {
    /// The input operator
    input: Arc<Mutex<dyn Operator + Send + Sync>>,
    /// The expressions to project (output names)
    expressions: Vec<(Expression, String)>,
}

impl ProjectionOperator {
    /// Create a new projection operator
    pub fn new(input: Arc<Mutex<dyn Operator + Send + Sync>>, expressions: Vec<(Expression, String)>) -> Self {
        ProjectionOperator { input, expressions }
    }
}

impl Operator for ProjectionOperator {
    /// Initialize the operator
    fn init(&mut self) -> QueryResult<()> {
        self.input.lock().unwrap().init()
    }
    
    /// Get the next row with projected columns
    fn next(&mut self) -> QueryResult<Option<Row>> {
        match self.input.lock().unwrap().next()? {
            Some(input_row) => {
                let mut output_values = LinkedHashMap::with_capacity(self.expressions.len());
                let mut column_order = Vec::with_capacity(self.expressions.len());

                for (expr, output_name) in &self.expressions {
                    // HACK: Pass None for schema. Expression evaluation needs to handle this.
                    let value = expression_eval::evaluate_expression(expr, &input_row, None)?;
                    output_values.insert(output_name.clone(), value);
                    column_order.push(output_name.clone());
                }
                Ok(Some(Row::from_ordered_map(output_values, column_order)))
            }
            None => Ok(None),
        }
    }
    
    /// Close the operator
    fn close(&mut self) -> QueryResult<()> {
        self.input.lock().unwrap().close()
    }
}

/// Create a projection operator
pub fn create_projection(
    input: Arc<Mutex<dyn Operator + Send + Sync>>,
    columns: Vec<String>,
    _input_alias: String,
) -> QueryResult<Arc<Mutex<dyn Operator + Send + Sync>>> {
    // For now, assume columns are simple column names from the input. 
    // A proper implementation needs to take Vec<(Expression, String)> from logical plan.
    let expressions: Vec<(Expression, String)> = columns.into_iter()
        .map(|name| (Expression::Column(crate::query::parser::ast::ColumnReference { table: None, name: name.clone() }), name))
        .collect();
    
    let op = ProjectionOperator::new(input, expressions);
    Ok(Arc::new(Mutex::new(op)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::executor::result::{DataValue, QueryError};
    
    // Mock operator for testing
    struct MockOperator {
        rows: Vec<Row>,
        index: usize,
        initialized: bool,
    }
    
    impl MockOperator {
        fn new(rows: Vec<Row>) -> Self {
            MockOperator {
                rows,
                index: 0,
                initialized: false,
            }
        }
    }
    
    impl Operator for MockOperator {
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
    
    // Helper function to create a test row
    fn create_test_row(id: i64, name: &str, age: i64) -> Row {
        let mut row = Row::new();
        row.set("id".to_string(), DataValue::Integer(id));
        row.set("name".to_string(), DataValue::Text(name.to_string()));
        row.set("age".to_string(), DataValue::Integer(age));
        row
    }
    
    #[test]
    fn test_projection_operator() {
        // Create test data
        let rows = vec![
            create_test_row(1, "Alice", 25),
            create_test_row(2, "Bob", 30),
            create_test_row(3, "Charlie", 35),
        ];
        
        // Create a mock operator
        let mock_op = MockOperator::new(rows);
        let mock_op_arc = Arc::new(Mutex::new(mock_op));
        
        // Create a projection operator with selected columns
        let columns = vec!["id".to_string(), "name".to_string()];
        let mut projection_op = ProjectionOperator::new(mock_op_arc, columns.into_iter().map(|name| (Expression::Column(crate::query::parser::ast::ColumnReference { table: None, name: name.clone() }), name)).collect());
        
        // Initialize and test
        projection_op.init().unwrap();
        
        // Check the projected rows
        for i in 1..=3 {
            let row = projection_op.next().unwrap().unwrap();
            
            // Should have only id and name columns
            assert_eq!(row.columns().len(), 2);
            assert!(row.get("id").is_some());
            assert!(row.get("name").is_some());
            assert!(row.get("age").is_none());
            
            // Verify values
            match i {
                1 => {
                    assert_eq!(row.get("id"), Some(&DataValue::Integer(1)));
                    assert_eq!(row.get("name"), Some(&DataValue::Text("Alice".to_string())));
                }
                2 => {
                    assert_eq!(row.get("id"), Some(&DataValue::Integer(2)));
                    assert_eq!(row.get("name"), Some(&DataValue::Text("Bob".to_string())));
                }
                3 => {
                    assert_eq!(row.get("id"), Some(&DataValue::Integer(3)));
                    assert_eq!(row.get("name"), Some(&DataValue::Text("Charlie".to_string())));
                }
                _ => panic!("Unexpected row"),
            }
        }
        
        // No more rows
        assert!(projection_op.next().unwrap().is_none());
        
        // Close the operator
        projection_op.close().unwrap();
    }
} 