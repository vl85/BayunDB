// Filter Operator Implementation
//
// This module implements the filter operator for filtering rows based on predicates.

use std::sync::{Arc, Mutex};

use crate::query::executor::operators::Operator;
use crate::query::executor::result::{Row, QueryResult, QueryError, DataValue};

/// Filter operator that filters rows based on a predicate
pub struct FilterOperator {
    /// The input operator
    input: Arc<Mutex<dyn Operator>>,
    /// The predicate to evaluate
    predicate: String, // For now, we'll use a simple string representation
    /// Whether the operator is initialized
    initialized: bool,
}

impl FilterOperator {
    /// Create a new filter operator
    pub fn new(input: Arc<Mutex<dyn Operator>>, predicate: String) -> Self {
        FilterOperator {
            input,
            predicate,
            initialized: false,
        }
    }
    
    /// Evaluate a simple predicate on a row
    /// 
    /// This is a simplified implementation for demonstration purposes.
    /// In a real database, you would have a proper expression evaluator.
    fn evaluate_predicate(&self, row: &Row) -> QueryResult<bool> {
        // For now, we'll implement a very simple predicate evaluator
        // that can handle basic column comparisons with constants
        
        // Example predicate format: "column > value"
        let parts: Vec<&str> = self.predicate.split_whitespace().collect();
        
        // Check if we have a simple binary comparison
        if parts.len() == 3 {
            let column_name = parts[0];
            let operator = parts[1];
            let value_str = parts[2];
            
            // Get the column value from the row
            let column_value = match row.get(column_name) {
                Some(value) => value,
                None => return Err(QueryError::ColumnNotFound(column_name.to_string())),
            };
            
            // Parse the constant value
            let constant_value = Self::parse_value(value_str)?;
            
            // Compare based on the operator
            match operator {
                "=" => Ok(column_value == &constant_value),
                ">" => Ok(self.compare_greater_than(column_value, &constant_value)),
                "<" => Ok(self.compare_less_than(column_value, &constant_value)),
                ">=" => Ok(self.compare_greater_equal(column_value, &constant_value)),
                "<=" => Ok(self.compare_less_equal(column_value, &constant_value)),
                "!=" => Ok(column_value != &constant_value),
                _ => Err(QueryError::ExecutionError(format!("Unsupported operator: {}", operator))),
            }
        } else {
            // For now, just accept everything if we can't understand the predicate
            // This is a simplification for this implementation
            Ok(true)
        }
    }
    
    /// Compare if a is greater than b, handling partial_cmp() correctly
    fn compare_greater_than(&self, a: &DataValue, b: &DataValue) -> bool {
        match a.partial_cmp(b) {
            Some(std::cmp::Ordering::Greater) => true,
            _ => false,
        }
    }
    
    /// Compare if a is less than b, handling partial_cmp() correctly
    fn compare_less_than(&self, a: &DataValue, b: &DataValue) -> bool {
        match a.partial_cmp(b) {
            Some(std::cmp::Ordering::Less) => true,
            _ => false,
        }
    }
    
    /// Compare if a is greater than or equal to b, handling partial_cmp() correctly
    fn compare_greater_equal(&self, a: &DataValue, b: &DataValue) -> bool {
        match a.partial_cmp(b) {
            Some(std::cmp::Ordering::Greater) | Some(std::cmp::Ordering::Equal) => true,
            _ => false,
        }
    }
    
    /// Compare if a is less than or equal to b, handling partial_cmp() correctly
    fn compare_less_equal(&self, a: &DataValue, b: &DataValue) -> bool {
        match a.partial_cmp(b) {
            Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal) => true,
            _ => false,
        }
    }
    
    /// Parse a string value into a DataValue
    fn parse_value(value_str: &str) -> QueryResult<DataValue> {
        // Try parsing as integer
        if let Ok(i) = value_str.parse::<i64>() {
            return Ok(DataValue::Integer(i));
        }
        
        // Try parsing as float
        if let Ok(f) = value_str.parse::<f64>() {
            return Ok(DataValue::Float(f));
        }
        
        // Handle string literals (quoted)
        if value_str.starts_with('"') && value_str.ends_with('"') {
            let s = value_str[1..value_str.len() - 1].to_string();
            return Ok(DataValue::Text(s));
        }
        
        // Handle boolean literals
        match value_str.to_lowercase().as_str() {
            "true" => return Ok(DataValue::Boolean(true)),
            "false" => return Ok(DataValue::Boolean(false)),
            "null" => return Ok(DataValue::Null),
            _ => {}
        }
        
        // Default to text
        Ok(DataValue::Text(value_str.to_string()))
    }
}

impl Operator for FilterOperator {
    /// Initialize the operator
    fn init(&mut self) -> QueryResult<()> {
        // Initialize the input operator
        {
            let mut input = self.input.lock().map_err(|e| {
                QueryError::ExecutionError(format!("Failed to lock input operator: {}", e))
            })?;
            
            input.init()?;
        }
        
        self.initialized = true;
        Ok(())
    }
    
    /// Get the next row that satisfies the predicate
    fn next(&mut self) -> QueryResult<Option<Row>> {
        if !self.initialized {
            return Err(QueryError::ExecutionError("Operator not initialized".to_string()));
        }
        
        // Keep fetching rows from the input operator until we find one that
        // satisfies the predicate or we run out of rows
        loop {
            // Get the next row from the input
            let next_row = {
                let mut input = self.input.lock().map_err(|e| {
                    QueryError::ExecutionError(format!("Failed to lock input operator: {}", e))
                })?;
                
                input.next()?
            };
            
            // If we've run out of rows, return None
            match next_row {
                None => return Ok(None),
                Some(row) => {
                    // Check if this row satisfies the predicate
                    if self.evaluate_predicate(&row)? {
                        return Ok(Some(row));
                    }
                    // Otherwise continue to the next row
                }
            }
        }
    }
    
    /// Close the operator
    fn close(&mut self) -> QueryResult<()> {
        self.initialized = false;
        
        // Close the input operator
        let mut input = self.input.lock().map_err(|e| {
            QueryError::ExecutionError(format!("Failed to lock input operator: {}", e))
        })?;
        
        input.close()
    }
}

/// Create a filter operator
pub fn create_filter(input: Arc<Mutex<dyn Operator>>, predicate: String) -> QueryResult<Arc<Mutex<dyn Operator>>> {
    let filter = FilterOperator::new(input, predicate);
    Ok(Arc::new(Mutex::new(filter)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::executor::result::DataValue;
    
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
    fn test_filter_operator() {
        // Create test data
        let rows = vec![
            create_test_row(1, "Alice", 25),
            create_test_row(2, "Bob", 30),
            create_test_row(3, "Charlie", 35),
            create_test_row(4, "Dave", 40),
        ];
        
        // Create a mock operator
        let mock_op = MockOperator::new(rows);
        let mock_op_arc = Arc::new(Mutex::new(mock_op));
        
        // Create a filter operator with a predicate
        let predicate = "age > 30".to_string();
        let mut filter_op = FilterOperator::new(mock_op_arc, predicate);
        
        // Initialize and test
        filter_op.init().unwrap();
        
        // First row should be Charlie (age 35)
        let row1 = filter_op.next().unwrap().unwrap();
        assert_eq!(row1.get("name"), Some(&DataValue::Text("Charlie".to_string())));
        assert_eq!(row1.get("age"), Some(&DataValue::Integer(35)));
        
        // Second row should be Dave (age 40)
        let row2 = filter_op.next().unwrap().unwrap();
        assert_eq!(row2.get("name"), Some(&DataValue::Text("Dave".to_string())));
        assert_eq!(row2.get("age"), Some(&DataValue::Integer(40)));
        
        // No more rows
        assert!(filter_op.next().unwrap().is_none());
        
        // Close the operator
        filter_op.close().unwrap();
    }
} 