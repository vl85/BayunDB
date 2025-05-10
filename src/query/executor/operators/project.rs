// Projection Operator Implementation
//
// This module implements the projection operator for selecting specific columns.

use std::sync::{Arc, Mutex};

use crate::query::executor::operators::Operator;
use crate::query::executor::result::{Row, QueryResult, QueryError};

/// Projection operator that selects specific columns from input rows
pub struct ProjectionOperator {
    /// The input operator
    input: Arc<Mutex<dyn Operator + Send>>,
    /// The columns to project
    columns: Vec<String>,
    /// Whether the operator is initialized
    initialized: bool,
}

impl ProjectionOperator {
    /// Create a new projection operator
    pub fn new(input: Arc<Mutex<dyn Operator + Send>>, columns: Vec<String>) -> Self {
        ProjectionOperator {
            input,
            columns,
            initialized: false,
        }
    }
    
    /// Project a row to only include the specified columns
    fn project_row(&self, row: Row) -> Row {
        let mut projected_row = Row::new();
        
        // If columns is empty or contains a wildcard, return the original row
        if self.columns.is_empty() || self.columns.contains(&"*".to_string()) {
            return row;
        }
        
        // Otherwise, only include the specified columns
        for column in &self.columns {
            if let Some(value) = row.get(column) {
                projected_row.set(column.clone(), value.clone());
            }
        }
        
        projected_row
    }
}

impl Operator for ProjectionOperator {
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
    
    /// Get the next row with projected columns
    fn next(&mut self) -> QueryResult<Option<Row>> {
        if !self.initialized {
            return Err(QueryError::ExecutionError("Operator not initialized".to_string()));
        }
        
        // Get the next row from the input
        let next_row = {
            let mut input = self.input.lock().map_err(|e| {
                QueryError::ExecutionError(format!("Failed to lock input operator: {}", e))
            })?;
            
            input.next()?
        };
        
        // If there's a row, project it
        match next_row {
            Some(row) => Ok(Some(self.project_row(row))),
            None => Ok(None),
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

/// Create a projection operator
pub fn create_projection(
    input: Arc<Mutex<dyn Operator + Send>>,
    columns: Vec<String>
) -> QueryResult<Arc<Mutex<dyn Operator + Send>>> {
    let projection = ProjectionOperator::new(input, columns);
    Ok(Arc::new(Mutex::new(projection)))
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
        let mut projection_op = ProjectionOperator::new(mock_op_arc, columns);
        
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