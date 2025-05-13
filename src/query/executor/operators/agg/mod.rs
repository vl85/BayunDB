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

impl AggregateType {
    /// Convert AST AggregateFunction to executor AggregateType
    pub fn from_ast_function(ast_func: &crate::query::parser::ast::AggregateFunction) -> crate::query::executor::result::QueryResult<Self> { // Assuming QueryResult is accessible
        match ast_func {
            crate::query::parser::ast::AggregateFunction::Count => Ok(AggregateType::Count),
            crate::query::parser::ast::AggregateFunction::Sum => Ok(AggregateType::Sum),
            crate::query::parser::ast::AggregateFunction::Avg => Ok(AggregateType::Avg),
            crate::query::parser::ast::AggregateFunction::Min => Ok(AggregateType::Min),
            crate::query::parser::ast::AggregateFunction::Max => Ok(AggregateType::Max),
            // Add handling for other potential AST aggregate variants if they exist, or return an error
            // Example: 
            // _ => Err(crate::query::executor::result::QueryError::ExecutionError(format!("Unsupported AST AggregateFunction: {:?}", ast_func)))
        }
    }
}

// Common utility functions for aggregate operators can be added here 