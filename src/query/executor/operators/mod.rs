// Query Operators Module
//
// This module defines the operators used for query execution in the
// iterator-based execution model.

// Re-export public components
pub mod scan;
pub mod filter;
pub mod project;

// Placeholder modules - will be implemented later
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

pub fn create_projection(input: Arc<Mutex<dyn Operator>>, columns: Vec<String>) -> QueryResult<Arc<Mutex<dyn Operator>>> {
    project::create_projection(input, columns)
} 