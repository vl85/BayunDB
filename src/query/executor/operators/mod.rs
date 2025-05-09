// Query Operators Module
//
// This module defines the operators used for query execution in the
// iterator-based execution model.

// Re-export public components
pub mod scan;
pub mod filter;
pub mod project;
pub mod join;

// Aggregation operators
pub mod agg {
    use std::sync::{Arc, Mutex};
    use crate::query::executor::result::{Row, QueryResult};
    use crate::query::executor::operators::Operator;
    
    /// Placeholder for HashAggregateOperator implementation
    /// This will be implemented in a future update
    pub struct HashAggregateOperator {
        // Input operator
        input: Arc<Mutex<dyn Operator>>,
        // Group by columns (expressions)
        group_by_columns: Vec<String>,
        // Aggregate expressions
        aggregate_expressions: Vec<String>,
        // Having clause (optional)
        having: Option<String>,
    }
    
    impl Operator for HashAggregateOperator {
        fn init(&mut self) -> QueryResult<()> {
            // Initialize input operator
            let mut input = self.input.lock().unwrap();
            input.init()
        }
        
        fn next(&mut self) -> QueryResult<Option<Row>> {
            // Placeholder - just returns from input for now
            let mut input = self.input.lock().unwrap();
            input.next()
        }
        
        fn close(&mut self) -> QueryResult<()> {
            // Close input operator
            let mut input = self.input.lock().unwrap();
            input.close()
        }
    }
    
    // Factory function to create a hash aggregate operator
    pub fn create_hash_aggregate(
        input: Arc<Mutex<dyn Operator>>,
        group_by_columns: Vec<String>,
        aggregate_expressions: Vec<String>,
        having: Option<String>
    ) -> QueryResult<Arc<Mutex<dyn Operator>>> {
        // Create and return the operator
        let operator = HashAggregateOperator {
            input,
            group_by_columns,
            aggregate_expressions,
            having,
        };
        
        Ok(Arc::new(Mutex::new(operator)))
    }
}

// Define the common Operator trait
use crate::query::executor::result::{Row, QueryResult};
use std::sync::{Arc, Mutex};

/// The Operator trait defines the interface for all query execution operators
/// in the iterator-based execution model. Each operator processes tuples and
/// passes them to the next operator in the execution plan.
pub trait Operator: Send + Sync {
    /// Initialize the operator before execution
    fn init(&mut self) -> QueryResult<()>;
    
    /// Get the next row of data from this operator
    fn next(&mut self) -> QueryResult<Option<Row>>;
    
    /// Close the operator and release any resources
    fn close(&mut self) -> QueryResult<()>;
}

// Factory functions for creating operators
pub fn create_table_scan(table_name: &str) -> QueryResult<Arc<Mutex<dyn Operator>>> {
    scan::create_table_scan(table_name)
}

pub fn create_filter(input: Arc<Mutex<dyn Operator>>, predicate: String) -> QueryResult<Arc<Mutex<dyn Operator>>> {
    filter::create_filter(input, predicate)
}

pub fn create_projection(input: Arc<Mutex<dyn Operator>>, columns: Vec<String>) -> QueryResult<Arc<Mutex<dyn Operator>>> {
    project::create_projection(input, columns)
}

pub fn create_nested_loop_join(
    left: Arc<Mutex<dyn Operator>>, 
    right: Arc<Mutex<dyn Operator>>, 
    condition: String,
    is_left_join: bool
) -> QueryResult<Arc<Mutex<dyn Operator>>> {
    join::create_nested_loop_join(left, right, condition, is_left_join)
}

pub fn create_hash_join(
    left: Arc<Mutex<dyn Operator>>, 
    right: Arc<Mutex<dyn Operator>>, 
    condition: String,
    is_left_join: bool
) -> QueryResult<Arc<Mutex<dyn Operator>>> {
    join::create_hash_join(left, right, condition, is_left_join)
}

pub fn create_hash_aggregate(
    input: Arc<Mutex<dyn Operator>>,
    group_by_columns: Vec<String>,
    aggregate_expressions: Vec<String>,
    having: Option<String>
) -> QueryResult<Arc<Mutex<dyn Operator>>> {
    agg::create_hash_aggregate(input, group_by_columns, aggregate_expressions, having)
} 