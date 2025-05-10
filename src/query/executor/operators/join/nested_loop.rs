// Nested Loop Join Implementation
//
// This file implements the nested loop join algorithm, which works for any join condition
// but has O(n*m) time complexity.

use std::sync::{Arc, Mutex};

use crate::query::executor::operators::Operator;
use crate::query::executor::result::{Row, QueryResult};

/// Nested Loop Join operator implementation
pub struct NestedLoopJoin {
    /// Left input operator
    left: Arc<Mutex<dyn Operator + Send>>,
    /// Right input operator
    right: Arc<Mutex<dyn Operator + Send>>,
    /// Join condition as a string representation (will be evaluated)
    condition: String,
    /// Alias for the left input
    left_alias: String,
    /// Alias for the right input
    right_alias: String,
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
        left: Arc<Mutex<dyn Operator + Send>>,
        right: Arc<Mutex<dyn Operator + Send>>,
        condition: String,
        is_left_join: bool,
        left_alias: String,
        right_alias: String,
    ) -> Self {
        NestedLoopJoin {
            left,
            right,
            condition,
            left_alias,
            right_alias,
            current_left_row: None,
            is_left_join,
            found_match: false,
            initialized: false,
        }
    }
    
    /// Evaluate the join condition between two rows
    fn evaluate_condition(&self, left_row: &Row, right_row: &Row) -> bool {
        // Crude parser for "[alias1.]col1 OP [alias2.]col2"
        // Handles =, >, <, >=, <=, !=

        let recognized_ops_with_len: Vec<(&str, usize)> = vec![
            (">=", 2), ("<=", 2), ("!=", 2),
            ("=", 1), (">", 1), ("<", 1),
        ];

        let mut operator: Option<&str> = None;
        let mut condition_parts: Option<(String, String)> = None;

        for (op_str, op_len) in recognized_ops_with_len {
            if let Some(op_idx) = self.condition.find(op_str) {
                operator = Some(op_str);
                let left_part = self.condition[..op_idx].trim().to_string();
                let right_part = self.condition[op_idx + op_len..].trim().to_string();
                condition_parts = Some((left_part, right_part));
                break;
            }
        }

        if operator.is_none() || condition_parts.is_none() {
            return false; 
        }

        let op = operator.unwrap();
        let (left_expr_str, right_expr_str) = condition_parts.unwrap();

        // The left_expr_str and right_expr_str are expected to be the fully qualified
        // column names, e.g., "t1.id" or "left_table.fk_id".
        // These should match the keys in the Row objects coming from TableScanOperator.
        let left_val = left_row.get(&left_expr_str); 
        let right_val = right_row.get(&right_expr_str);

        match (left_val, right_val) {
            (Some(l), Some(r)) => {
                match op {
                    "=" => l == r,
                    ">" => l > r,
                    "<" => l < r,
                    ">=" => l >= r,
                    "<=" => l <= r,
                    "!=" => l != r,
                    _ => false, 
                }
            }
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
                    if self.evaluate_condition(self.current_left_row.as_ref().unwrap(), &right) {
                        self.found_match = true;
                        
                        let mut joined_row = Row::new();
                        let left_row_ref = self.current_left_row.as_ref().unwrap();

                        // Add all columns from the left_row. Names are already qualified by TableScanOperator.
                        for left_col_key in left_row_ref.columns() { // left_col_key is e.g., "t1.id"
                            if let Some(value) = left_row_ref.get(left_col_key) {
                                joined_row.set(left_col_key.clone(), value.clone());
                            }
                        }
                        
                        // Add all columns from the right row. Names are already qualified.
                        for right_col_key in right.columns() { // right_col_key is e.g., "t2.fk_id"
                            if let Some(value) = right.get(right_col_key) {
                                // Simple set; assumes TableScanOperator aliases make keys unique.
                                // If t1.name and t2.name both exist, they are distinct keys.
                                joined_row.set(right_col_key.clone(), value.clone());
                            }
                        }
                        
                        return Ok(Some(joined_row));
                    }
                }
                None => { // Reached the end of right relation
                    let mut emit_unmatched_left = false;
                    if self.is_left_join && !self.found_match {
                        if self.current_left_row.is_some() { 
                            emit_unmatched_left = true;
                        }
                    }

                    if emit_unmatched_left {
                        let mut joined_row = Row::new();
                        if let Some(left_row_ref) = self.current_left_row.as_ref() {
                            // Add all columns from the left_row for the unmatched case.
                            for left_col_key in left_row_ref.columns() {
                                if let Some(value) = left_row_ref.get(left_col_key) {
                                    joined_row.set(left_col_key.clone(), value.clone());
                                }
                            }
                            // For a true LEFT JOIN, we'd add NULLs for all columns of the right table's schema here.
                            // This requires knowing the right table's schema (column names and aliases).
                            // For now, it just includes the left side.
                        }
                        
                        // Advance to the next left row and reset right side *before* returning unmatched row
                        self.current_left_row = self.left.lock().unwrap().next()?;
                        self.found_match = false; 
                        if self.current_left_row.is_some() { 
                            self.right.lock().unwrap().close()?;
                            self.right.lock().unwrap().init()?;
                        } else { 
                            self.right.lock().unwrap().close()?;
                        }
                        return Ok(Some(joined_row)); 
                    }
                    
                    self.current_left_row = self.left.lock().unwrap().next()?;
                    self.found_match = false; 
                    
                    if self.current_left_row.is_none() {
                        return Ok(None); 
                    } else {
                        self.right.lock().unwrap().close()?;
                        self.right.lock().unwrap().init()?;
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
    use std::sync::{Arc, Mutex};
    use crate::query::executor::result::{DataValue, Row, QueryResult};
    use crate::query::executor::operators::Operator;

    // Local MockOperator for these tests to ensure qualified column names
    #[derive(Clone)]
    pub struct LocalMockOperator {
        rows: Vec<Row>,
        iter_count: usize,
    }

    impl LocalMockOperator {
        pub fn new(rows: Vec<Row>) -> Self {
            LocalMockOperator { rows, iter_count: 0 }
        }
    }

    impl Operator for LocalMockOperator {
        fn init(&mut self) -> QueryResult<()> { self.iter_count = 0; Ok(()) }
        fn next(&mut self) -> QueryResult<Option<Row>> {
            if self.iter_count < self.rows.len() {
                self.iter_count += 1;
                Ok(Some(self.rows[self.iter_count - 1].clone()))
            } else {
                Ok(None)
            }
        }
        fn close(&mut self) -> QueryResult<()> { Ok(()) }
    }

    // Local helper to create a test row with qualified column names
    fn create_test_row_qualified(id: i64, name: &str, alias: &str) -> Row {
        let mut row = Row::new();
        row.set(format!("{}.id", alias), DataValue::Integer(id));
        row.set(format!("{}.name", alias), DataValue::Text(name.to_string()));
        row
    }

    // Local helper to create an order row with qualified column names
    fn create_order_row_qualified(id: i64, order_id: i64, alias: &str) -> Row {
        let mut row = Row::new();
        row.set(format!("{}.id", alias), DataValue::Integer(id));
        row.set(format!("{}.order_id", alias), DataValue::Integer(order_id));
        row
    }

    #[test]
    fn test_nested_loop_join() {
        let left_alias_str = "left_alias";
        let right_alias_str = "right_alias";

        // Create test data with qualified column names
        let left_rows = vec![
            create_test_row_qualified(1, "Alice", left_alias_str),
            create_test_row_qualified(2, "Bob", left_alias_str),
            create_test_row_qualified(3, "Charlie", left_alias_str),
        ];
        
        let right_rows = vec![
            create_order_row_qualified(1, 101, right_alias_str), // Corresponds to left_alias.id = 1
            create_order_row_qualified(2, 102, right_alias_str), // Corresponds to left_alias.id = 2
        ];
        
        // Create operators using LocalMockOperator
        let left_op = Arc::new(Mutex::new(LocalMockOperator::new(left_rows)));
        let right_op = Arc::new(Mutex::new(LocalMockOperator::new(right_rows)));
        
        // Create join operator with a condition using qualified names
        let mut join_op = NestedLoopJoin::new(
            left_op.clone(),
            right_op.clone(),
            format!("{}.id = {}.id", left_alias_str, right_alias_str), // Condition on qualified "id"
            false,
            left_alias_str.to_string(),
            right_alias_str.to_string(),
        );
        
        // Initialize the operator
        join_op.init().unwrap();
        
        // Get joined rows
        let row1 = join_op.next().unwrap().unwrap();
        assert_eq!(row1.get(&format!("{}.id", left_alias_str)), Some(&DataValue::Integer(1)));
        assert_eq!(row1.get(&format!("{}.name", left_alias_str)), Some(&DataValue::Text("Alice".to_string())));
        assert_eq!(row1.get(&format!("{}.id", right_alias_str)), Some(&DataValue::Integer(1))); // from right_rows' "id"
        assert_eq!(row1.get(&format!("{}.order_id", right_alias_str)), Some(&DataValue::Integer(101)));
        
        let row2 = join_op.next().unwrap().unwrap();
        assert_eq!(row2.get(&format!("{}.id", left_alias_str)), Some(&DataValue::Integer(2)));
        assert_eq!(row2.get(&format!("{}.name", left_alias_str)), Some(&DataValue::Text("Bob".to_string())));
        assert_eq!(row2.get(&format!("{}.id", right_alias_str)), Some(&DataValue::Integer(2))); // from right_rows' "id"
        assert_eq!(row2.get(&format!("{}.order_id", right_alias_str)), Some(&DataValue::Integer(102)));
        
        // No more joined rows
        assert!(join_op.next().unwrap().is_none());
    }
    
    #[test]
    fn test_nested_loop_left_join() {
        let left_alias_str = "left_alias";
        let right_alias_str = "right_alias";

        // Create test data for LEFT JOIN scenario with qualified names
        let left_rows = vec![
            create_test_row_qualified(1, "Alice", left_alias_str),
            create_test_row_qualified(2, "Bob", left_alias_str),
            create_test_row_qualified(3, "Charlie", left_alias_str), // No matching right row
        ];
        
        let right_rows = vec![
            create_order_row_qualified(1, 101, right_alias_str),
            create_order_row_qualified(2, 102, right_alias_str),
        ];
        
        // Create operators
        let left_op = Arc::new(Mutex::new(LocalMockOperator::new(left_rows)));
        let right_op = Arc::new(Mutex::new(LocalMockOperator::new(right_rows)));
        
        // Create nested loop join operator with LEFT JOIN and qualified condition
        let mut join_op = NestedLoopJoin::new(
            left_op.clone(),
            right_op.clone(),
            format!("{}.id = {}.id", left_alias_str, right_alias_str), // Condition on qualified "id"
            true, // is_left_join = true
            left_alias_str.to_string(),
            right_alias_str.to_string(),
        );
        
        // Initialize the operator
        join_op.init().unwrap();
        
        // Get joined rows
        let row1 = join_op.next().unwrap().unwrap();
        assert_eq!(row1.get(&format!("{}.id", left_alias_str)), Some(&DataValue::Integer(1)));
        assert_eq!(row1.get(&format!("{}.name", left_alias_str)), Some(&DataValue::Text("Alice".to_string())));
        assert_eq!(row1.get(&format!("{}.id", right_alias_str)), Some(&DataValue::Integer(1)));
        assert_eq!(row1.get(&format!("{}.order_id", right_alias_str)), Some(&DataValue::Integer(101)));
        
        let row2 = join_op.next().unwrap().unwrap();
        assert_eq!(row2.get(&format!("{}.id", left_alias_str)), Some(&DataValue::Integer(2)));
        assert_eq!(row2.get(&format!("{}.name", left_alias_str)), Some(&DataValue::Text("Bob".to_string())));
        assert_eq!(row2.get(&format!("{}.id", right_alias_str)), Some(&DataValue::Integer(2)));
        assert_eq!(row2.get(&format!("{}.order_id", right_alias_str)), Some(&DataValue::Integer(102)));
        
        // Third row should have Charlie with NULL for right side columns (as per TODO in next())
        let row3 = join_op.next().unwrap().unwrap();
        assert_eq!(row3.get(&format!("{}.id", left_alias_str)), Some(&DataValue::Integer(3)));
        assert_eq!(row3.get(&format!("{}.name", left_alias_str)), Some(&DataValue::Text("Charlie".to_string())));
        // Due to the current limitation (TODO in next()), right side columns will be absent for unmatched left rows.
        // If/when NULL padding is implemented, these should be Some(&DataValue::Null)
        assert_eq!(row3.get(&format!("{}.id", right_alias_str)), None); 
        assert_eq!(row3.get(&format!("{}.order_id", right_alias_str)), None);
        
        // No more joined rows
        assert!(join_op.next().unwrap().is_none());
    }
    
    #[test]
    fn test_nested_loop_join_non_equality() {
        let left_alias_str = "left_alias";
        let right_alias_str = "right_alias";

        // Test with a non-equality condition
        let left_rows = vec![
            create_test_row_qualified(2, "Bob", left_alias_str), // L.id = 2
            create_test_row_qualified(3, "Charlie", left_alias_str), // L.id = 3
        ];
        
        let right_rows = vec![
            create_order_row_qualified(1, 101, right_alias_str), // R.id = 1
            create_order_row_qualified(2, 102, right_alias_str), // R.id = 2
        ];
        
        // Create operators
        let left_op = Arc::new(Mutex::new(LocalMockOperator::new(left_rows)));
        let right_op = Arc::new(Mutex::new(LocalMockOperator::new(right_rows)));
        
        // Create join operator with non-equality condition using qualified names
        let mut join_op = NestedLoopJoin::new(
            left_op.clone(),
            right_op.clone(),
            format!("{}.id > {}.id", left_alias_str, right_alias_str), // Greater than condition on qualified names
            false,
            left_alias_str.to_string(),
            right_alias_str.to_string(),
        );
        
        assert!(join_op.init().is_ok());

        // Let's test the actual output for non-equality
        // Expected matches:
        // (L.id=2, R.id=1)
        // (L.id=3, R.id=1)
        // (L.id=3, R.id=2)
        let mut count = 0;
        while let Some(row) = join_op.next().unwrap() {
            count += 1;
            let left_id_val = match row.get(&format!("{}.id", left_alias_str)) {
                Some(DataValue::Integer(val)) => *val,
                _ => panic!("Expected Integer for {}.id or key not found/wrong type", left_alias_str),
            };
            let right_id_val = match row.get(&format!("{}.id", right_alias_str)) {
                Some(DataValue::Integer(val)) => *val,
                _ => panic!("Expected Integer for {}.id or key not found/wrong type", right_alias_str),
            };
            assert!(left_id_val > right_id_val);

            if left_id_val == 2 { // Bob
                assert_eq!(right_id_val, 1); // Bob.id (2) > Order.id (1)
            } else if left_id_val == 3 { // Charlie
                assert!(right_id_val == 1 || right_id_val == 2); // Charlie.id (3) > Order.id (1 or 2)
            } else {
                panic!("Unexpected left_id_val in non-equality join result");
            }
        }
        assert_eq!(count, 3, "Expected 3 rows from non-equality join L.id > R.id");
        
        assert!(join_op.close().is_ok());
    }
} 