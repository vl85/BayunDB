// Query Executor Module
//
// This module is responsible for executing query plans and producing results.
// It implements the iterator-based execution model for query processing.

// Re-export public components
pub mod engine;
pub mod result;
pub mod operators;

// Export key types
pub use self::engine::ExecutionEngine;
pub use self::result::QueryResult;
pub use self::operators::Operator; 