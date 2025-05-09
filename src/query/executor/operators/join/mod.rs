// Join Operators Module
//
// This module defines the join operators for query execution in the
// iterator-based execution model.

// Re-export public components
pub use self::nested_loop::NestedLoopJoin;
pub use self::hash_join::HashJoin;

// Import other modules
mod nested_loop;
mod hash_join;

use std::sync::{Arc, Mutex};
use crate::query::executor::operators::Operator;
use crate::query::executor::result::QueryResult;

/// Create a nested loop join operator
pub fn create_nested_loop_join(
    left: Arc<Mutex<dyn Operator>>,
    right: Arc<Mutex<dyn Operator>>,
    condition: String,
    is_left_join: bool,
) -> QueryResult<Arc<Mutex<dyn Operator>>> {
    Ok(Arc::new(Mutex::new(NestedLoopJoin::new(
        left, right, condition, is_left_join
    ))))
}

/// Create a hash join operator
pub fn create_hash_join(
    left: Arc<Mutex<dyn Operator>>,
    right: Arc<Mutex<dyn Operator>>,
    condition: String,
    is_left_join: bool,
) -> QueryResult<Arc<Mutex<dyn Operator>>> {
    Ok(Arc::new(Mutex::new(HashJoin::new(
        left, right, condition, is_left_join
    ))))
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::query::executor::result::{Row, DataValue};

    // Mock operator for testing
    pub struct MockOperator {
        rows: Vec<Row>,
        index: usize,
        initialized: bool,
    }
    
    impl MockOperator {
        pub fn new(rows: Vec<Row>) -> Self {
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
                self.init()?;
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
    
    // Helper to create test rows
    pub fn create_test_row(id: i64, name: &str) -> Row {
        let mut row = Row::new();
        row.set("id".to_string(), DataValue::Integer(id));
        row.set("name".to_string(), DataValue::Text(name.to_string()));
        row
    }
    
    // Helper to create order rows
    pub fn create_order_row(id: i64, order_id: i64) -> Row {
        let mut row = Row::new();
        row.set("id".to_string(), DataValue::Integer(id));
        row.set("order_id".to_string(), DataValue::Integer(order_id));
        row
    }
} 