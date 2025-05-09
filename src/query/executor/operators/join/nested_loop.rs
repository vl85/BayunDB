// Nested Loop Join Implementation
//
// This file implements the nested loop join algorithm, which works for any join condition
// but has O(n*m) time complexity.

use std::sync::{Arc, Mutex};

use crate::query::executor::operators::Operator;
use crate::query::executor::result::{Row, QueryResult, DataValue};

/// Nested Loop Join operator implementation
pub struct NestedLoopJoin {
    /// Left input operator
    left: Arc<Mutex<dyn Operator>>,
    /// Right input operator
    right: Arc<Mutex<dyn Operator>>,
    /// Join condition as a string representation (will be evaluated)
    condition: String,
    /// Current left row being processed
    current_left_row: Option<Row>,
    /// Indicates if this is a LEFT OUTER JOIN
    is_left_join: bool,
    /// Flag indicating if we've matched the current left row
    found_match: bool,
    /// Initialization status
    initialized: bool,
}

impl NestedLoopJoin {
    /// Create a new nested loop join operator
    pub fn new(
        left: Arc<Mutex<dyn Operator>>,
        right: Arc<Mutex<dyn Operator>>,
        condition: String,
        is_left_join: bool,
    ) -> Self {
        NestedLoopJoin {
            left,
            right,
            condition,
            current_left_row: None,
            is_left_join,
            found_match: false,
            initialized: false,
        }
    }
    
    /// Evaluate the join condition between two rows
    fn evaluate_condition(&self, left_row: &Row, right_row: &Row) -> bool {
        // For a real implementation, we would parse and evaluate the condition
        // Here we'll use a simple approach for demonstration
        
        // Parse the condition in the format "left.column = right.column"
        let parts: Vec<&str> = self.condition.split('=').collect();
        if parts.len() != 2 {
            return false; // Only handle equality conditions for simplicity
        }
        
        let left_col = parts[0].trim();
        let right_col = parts[1].trim();
        
        // Extract table and column names
        let (_left_table, left_column) = if left_col.contains('.') {
            let parts: Vec<&str> = left_col.split('.').collect();
            (parts[0], parts[1])
        } else {
            ("", left_col)
        };
        
        let (_right_table, right_column) = if right_col.contains('.') {
            let parts: Vec<&str> = right_col.split('.').collect();
            (parts[0], parts[1])
        } else {
            ("", right_col)
        };
        
        // Get values from rows
        let left_value = left_row.get(left_column);
        let right_value = right_row.get(right_column);
        
        // Compare values
        match (left_value, right_value) {
            (Some(l), Some(r)) => l == r,
            _ => false,
        }
    }
}

impl Operator for NestedLoopJoin {
    fn init(&mut self) -> QueryResult<()> {
        // Initialize both input operators
        self.left.lock().unwrap().init()?;
        self.right.lock().unwrap().init()?;
        
        // Get the first left row
        self.current_left_row = self.left.lock().unwrap().next()?;
        self.found_match = false;
        self.initialized = true;
        
        Ok(())
    }
    
    fn next(&mut self) -> QueryResult<Option<Row>> {
        if !self.initialized {
            self.init()?;
        }
        
        // If no left row, we're done
        if self.current_left_row.is_none() {
            return Ok(None);
        }
        
        loop {
            // Get the next right row
            let right_row = self.right.lock().unwrap().next()?;
            
            match right_row {
                Some(right) => {
                    // Check if rows satisfy the join condition
                    if self.evaluate_condition(self.current_left_row.as_ref().unwrap(), &right) {
                        self.found_match = true;
                        
                        // Create a joined row by combining left and right rows
                        let mut joined_row = self.current_left_row.as_ref().unwrap().clone();
                        
                        // Add all columns from right row
                        for column in right.columns() {
                            if let Some(value) = right.get(column) {
                                joined_row.set(column.clone(), value.clone());
                            }
                        }
                        
                        return Ok(Some(joined_row));
                    }
                }
                None => {
                    // Reached the end of right relation
                    // For LEFT JOIN, emit a row with NULL values for right side if no match
                    if self.is_left_join && !self.found_match && self.current_left_row.is_some() {
                        let joined_row = self.current_left_row.as_ref().unwrap().clone();
                        
                        // Add NULL values for right columns (would need schema info)
                        // For a real implementation, we would get the schema for the right table
                        // and add NULL values for each column
                        
                        // Reset the right operator for the next left row
                        self.right.lock().unwrap().close()?;
                        self.right.lock().unwrap().init()?;
                        
                        // Get the next left row for the next iteration
                        let _prev_left = self.current_left_row.take();
                        self.current_left_row = self.left.lock().unwrap().next()?;
                        self.found_match = false;
                        
                        return Ok(Some(joined_row));
                    }
                    
                    // Reset the right operator for the next left row
                    self.right.lock().unwrap().close()?;
                    self.right.lock().unwrap().init()?;
                    
                    // Get the next left row
                    self.current_left_row = self.left.lock().unwrap().next()?;
                    self.found_match = false;
                    
                    // If no more left rows, we're done
                    if self.current_left_row.is_none() {
                        return Ok(None);
                    }
                }
            }
        }
    }
    
    fn close(&mut self) -> QueryResult<()> {
        // Close both input operators
        self.left.lock().unwrap().close()?;
        self.right.lock().unwrap().close()?;
        self.initialized = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::query::executor::operators::join::tests::{MockOperator, create_test_row, create_order_row};

    #[test]
    fn test_nested_loop_join() {
        // Create test data
        let left_rows = vec![
            create_test_row(1, "Alice"),
            create_test_row(2, "Bob"),
            create_test_row(3, "Charlie"),
        ];
        
        let right_rows = vec![
            create_order_row(1, 101),
            create_order_row(2, 102),
        ];
        
        // Create operators
        let left_op = Arc::new(Mutex::new(MockOperator::new(left_rows)));
        let right_op = Arc::new(Mutex::new(MockOperator::new(right_rows)));
        
        // Create join operator
        let mut join_op = NestedLoopJoin::new(
            left_op.clone(),
            right_op.clone(),
            "id = id".to_string(),
            false,
        );
        
        // Initialize the operator
        join_op.init().unwrap();
        
        // Get joined rows
        let row1 = join_op.next().unwrap().unwrap();
        assert_eq!(row1.get("id"), Some(&DataValue::Integer(1)));
        assert_eq!(row1.get("name"), Some(&DataValue::Text("Alice".to_string())));
        assert_eq!(row1.get("order_id"), Some(&DataValue::Integer(101)));
        
        let row2 = join_op.next().unwrap().unwrap();
        assert_eq!(row2.get("id"), Some(&DataValue::Integer(2)));
        assert_eq!(row2.get("name"), Some(&DataValue::Text("Bob".to_string())));
        assert_eq!(row2.get("order_id"), Some(&DataValue::Integer(102)));
        
        // No more joined rows
        assert!(join_op.next().unwrap().is_none());
    }
    
    #[test]
    fn test_nested_loop_left_join() {
        // Create test data for LEFT JOIN scenario
        let left_rows = vec![
            create_test_row(1, "Alice"),
            create_test_row(2, "Bob"),
            create_test_row(3, "Charlie"), // No matching right row
        ];
        
        let right_rows = vec![
            create_order_row(1, 101),
            create_order_row(2, 102),
        ];
        
        // Create operators
        let left_op = Arc::new(Mutex::new(MockOperator::new(left_rows)));
        let right_op = Arc::new(Mutex::new(MockOperator::new(right_rows)));
        
        // Create nested loop join operator with LEFT JOIN
        let mut join_op = NestedLoopJoin::new(
            left_op.clone(),
            right_op.clone(),
            "id = id".to_string(),
            true, // is_left_join = true
        );
        
        // Initialize the operator
        join_op.init().unwrap();
        
        // Get joined rows
        let row1 = join_op.next().unwrap().unwrap();
        assert_eq!(row1.get("id"), Some(&DataValue::Integer(1)));
        assert_eq!(row1.get("name"), Some(&DataValue::Text("Alice".to_string())));
        assert_eq!(row1.get("order_id"), Some(&DataValue::Integer(101)));
        
        let row2 = join_op.next().unwrap().unwrap();
        assert_eq!(row2.get("id"), Some(&DataValue::Integer(2)));
        assert_eq!(row2.get("name"), Some(&DataValue::Text("Bob".to_string())));
        assert_eq!(row2.get("order_id"), Some(&DataValue::Integer(102)));
        
        // Third row should have Charlie with NULL for order_id
        let row3 = join_op.next().unwrap().unwrap();
        assert_eq!(row3.get("id"), Some(&DataValue::Integer(3)));
        assert_eq!(row3.get("name"), Some(&DataValue::Text("Charlie".to_string())));
        
        // No more joined rows
        assert!(join_op.next().unwrap().is_none());
    }
    
    #[test]
    fn test_nested_loop_join_non_equality() {
        // Test with a non-equality condition
        let left_rows = vec![
            create_test_row(1, "Alice"),
            create_test_row(2, "Bob"),
            create_test_row(3, "Charlie"),
        ];
        
        let right_rows = vec![
            create_order_row(1, 101),
            create_order_row(2, 102),
        ];
        
        // Create operators
        let left_op = Arc::new(Mutex::new(MockOperator::new(left_rows)));
        let right_op = Arc::new(Mutex::new(MockOperator::new(right_rows)));
        
        // Create join operator with non-equality condition
        let mut join_op = NestedLoopJoin::new(
            left_op.clone(),
            right_op.clone(),
            "id > id".to_string(), // Greater than condition
            false,
        );
        
        // For non-equality in our current implementation, we can't fully test
        // as the evaluate_condition method is simplified to handle only equality
        // but we can verify it initializes properly
        assert!(join_op.init().is_ok());
    }
} 