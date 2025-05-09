// Aggregation Operators Module
//
// This module contains operators for performing SQL aggregation operations
// such as GROUP BY, HAVING, and aggregate functions (COUNT, SUM, AVG, etc.)

mod hash;
mod sort;

// Re-export public components
pub use hash::{HashAggregateOperator, create_hash_aggregate};
pub use sort::{SortAggregateOperator, create_sort_aggregate};

// Types of supported aggregate functions
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateType {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

// Common utility functions for aggregate operators can be added here 