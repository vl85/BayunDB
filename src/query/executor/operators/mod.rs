// Query Operators Module
//
// This module defines the operators used for query execution in the
// iterator-based execution model.

// Re-export public components
pub mod scan;
pub mod filter;
pub mod project;
pub mod join;
pub mod agg;
pub mod sort;
pub mod dummy;

// Define the common Operator trait
use crate::query::executor::result::{Row, QueryResult};
use std::sync::{Arc, Mutex, RwLock};
use crate::query::parser::ast::Expression;
use crate::storage::buffer::BufferPoolManager;
use crate::catalog::Catalog;

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

/// A dummy operator that doesn't produce any results
/// Used for DDL operations like CREATE TABLE that don't return rows.
/// 
/// IMPORTANT: This is a placeholder operator and does NOT execute the actual DDL operation.
/// DDL operations should be routed to the correct execution handler (e.g., ExecutionEngine::execute_create)
/// rather than attempting to execute them through the operator tree.
/// 
/// The DummyOperator exists to maintain the interface consistency in the operator tree model,
/// allowing DDL operations to fit into the physical plan representation even though
/// they don't naturally map to the iterator model used by query operators.
pub struct DummyOperator;

impl Default for DummyOperator {
    fn default() -> Self {
        Self::new()
    }
}

impl DummyOperator {
    /// Create a new dummy operator
    pub fn new() -> Self {
        DummyOperator
    }
}

impl Operator for DummyOperator {
    fn init(&mut self) -> QueryResult<()> {
        Ok(())
    }
    
    fn next(&mut self) -> QueryResult<Option<Row>> {
        // Dummy operator doesn't produce any rows
        Ok(None)
    }
    
    fn close(&mut self) -> QueryResult<()> {
        Ok(())
    }
}

// Factory functions for creating operators
pub fn create_table_scan(
    table_name: &str, 
    alias: &str, 
    buffer_pool: Arc<BufferPoolManager>,
    catalog: Arc<RwLock<Catalog>>
) -> QueryResult<Arc<Mutex<dyn Operator + Send + Sync>>> {
    scan::create_table_scan(table_name, alias, buffer_pool, catalog)
}

/// Create a filter operator that filters rows based on a predicate expression
pub fn create_filter(
    input: Arc<Mutex<dyn Operator + Send + Sync>>,
    predicate: Expression,
    _input_alias: String,
) -> QueryResult<Arc<Mutex<dyn Operator + Send + Sync>>> {
    Ok(Arc::new(Mutex::new(filter::FilterOperator::new(
        input,
        predicate,
    ))))
}

pub fn create_projection(input: Arc<Mutex<dyn Operator + Send + Sync>>, columns: Vec<String>, input_alias: String) -> QueryResult<Arc<Mutex<dyn Operator + Send + Sync>>> {
    project::create_projection(input, columns, input_alias)
}

pub fn create_nested_loop_join(
    left: Arc<Mutex<dyn Operator + Send + Sync>>, 
    right: Arc<Mutex<dyn Operator + Send + Sync>>, 
    condition: String,
    is_left_join: bool,
    left_alias: String,
    right_alias: String
) -> QueryResult<Arc<Mutex<dyn Operator + Send + Sync>>> {
    join::create_nested_loop_join(left, right, condition, is_left_join, left_alias, right_alias)
}

pub fn create_hash_join(
    left: Arc<Mutex<dyn Operator + Send + Sync>>, 
    right: Arc<Mutex<dyn Operator + Send + Sync>>, 
    condition: String,
    is_left_join: bool,
    left_alias: String,
    right_alias: String,
) -> QueryResult<Arc<Mutex<dyn Operator + Send + Sync>>> {
    join::create_hash_join(left, right, condition, is_left_join, left_alias, right_alias)
}

/// Create a sort operator that sorts rows based on order by expressions
pub fn create_sort(
    input: Arc<Mutex<dyn Operator + Send + Sync>>,
    order_by_expressions: Vec<(Expression, bool)> // (Expression, is_desc)
) -> QueryResult<Arc<Mutex<dyn Operator + Send + Sync>>> {
    sort::create_sort_operator(input, order_by_expressions)
}

// Re-export create_hash_aggregate from the agg::hash module
// This single `pub mod agg;` declaration and the use inside it are correct.
// The builder calls operators::agg::create_hash_aggregate directly.

// If we *did* want a wrapper here, it would need the updated signature:
/*
pub fn create_hash_aggregate(
    input: Arc<Mutex<dyn Operator + Send + Sync>>,
    group_by_expressions: Vec<crate::query::parser::ast::Expression>,
    aggregate_select_expressions: Vec<crate::query::planner::physical_plan::PhysicalSelectExpression>,
    having_expression: Option<crate::query::parser::ast::Expression>,
    output_select_list: Vec<crate::query::planner::physical_plan::PhysicalSelectExpression>,
) -> QueryResult<Arc<Mutex<dyn Operator + Send + Sync>>> {
    // Call the actual implementation
    agg::hash::create_hash_aggregate(
        input,
        group_by_expressions,
        aggregate_select_expressions,
        having_expression,
        output_select_list
    )
}
*/ 