// Query Operators Module
//
// This module defines the operators used for query execution in the
// iterator-based execution model.

// Re-export public components
pub mod scan;
// Placeholder modules - will be implemented later
pub mod filter {
    use std::sync::{Arc, Mutex};
    use crate::query::executor::operators::Operator;
    use crate::query::executor::result::QueryResult;
    
    pub fn create_filter(_input: Arc<Mutex<dyn Operator>>, _predicate: String) -> QueryResult<Arc<Mutex<dyn Operator>>> {
        unimplemented!("Filter operator not yet implemented")
    }
}

pub mod join {
    // Placeholder implementation
}

pub mod agg {
    // Placeholder implementation
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