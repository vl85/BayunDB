// Query Executor Module
//
// This module is responsible for executing query plans and producing results.
// It implements the iterator-based execution model for query processing.

// Re-export public components
pub mod expression_eval;
pub mod operators;
pub mod result;

// Engine components
pub mod engine;
pub mod dml_executor;
pub mod ddl_executor;
pub mod type_conversion;

// Re-export key types
pub use engine::ExecutionEngine;
pub use self::result::QueryResult;
pub use operators::Operator; 