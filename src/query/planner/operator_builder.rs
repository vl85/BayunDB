// Operator Builder
//
// This module builds executable operator trees from physical plans.

use std::sync::{Arc, Mutex};
use std::sync::RwLock;

use crate::query::executor::operators::{
    Operator,
    create_table_scan,
    create_filter,
    create_projection,
    create_nested_loop_join,
    create_hash_join,
    create_hash_aggregate
};
use crate::query::executor::result::QueryResult;
use crate::query::planner::physical_plan::PhysicalPlan;
use crate::query::parser::ast::JoinType;
use crate::query::planner::physical_plan::expression_to_predicate;
use crate::storage::buffer::BufferPoolManager;
use crate::catalog::Catalog;

/// Converts a JoinType to the is_left_join boolean flag expected by operators
fn join_type_to_is_left_join(join_type: &JoinType) -> bool {
    match join_type {
        JoinType::LeftOuter => true,
        _ => false
    }
}

/// Helper function to get an alias for a plan node, recursing through Filters.
fn get_plan_alias(plan: &PhysicalPlan, default_prefix: &str) -> String {
    match plan {
        PhysicalPlan::SeqScan { table_name, alias } => {
            alias.clone().unwrap_or_else(|| table_name.clone())
        }
        PhysicalPlan::Filter { input, .. } => {
            // Recurse into the filter's input to find the source alias
            get_plan_alias(input, default_prefix)
        }
        // TODO: Consider how to handle aliases for other input types like Joins or Aggregates.
        // A projection over a join might need to qualify columns with the join's output aliases if any,
        // or handle columns from different original tables. For now, this is a simplification.
        _ => format!("{}_derived_input", default_prefix), 
    }
}

/// Builds executable operator trees from physical plans
pub struct OperatorBuilder {
    /// Buffer pool manager for storage access
    buffer_pool: Arc<BufferPoolManager>,
    /// Catalog for schema information
    catalog: Arc<RwLock<Catalog>>,
}

impl OperatorBuilder {
    /// Create a new operator builder
    pub fn new(buffer_pool: Arc<BufferPoolManager>, catalog: Arc<RwLock<Catalog>>) -> Self {
        OperatorBuilder {
            buffer_pool,
            catalog,
        }
    }
    
    /// Build an operator tree from a physical plan
    pub fn build_operator_tree(&self, plan: &PhysicalPlan) -> QueryResult<Arc<Mutex<dyn Operator + Send>>> {
        match plan {
            PhysicalPlan::SeqScan { table_name, alias } => {
                let alias_str = alias.clone().unwrap_or_else(|| table_name.clone());
                // Pass the catalog to create_table_scan
                create_table_scan(table_name, &alias_str, self.buffer_pool.clone(), self.catalog.clone())
            }
            
            PhysicalPlan::Filter { input, predicate } => {
                // Build the input operator
                let input_op = self.build_operator_tree(input)?;
                // Get the alias of the input plan node
                let input_alias = get_plan_alias(input, "input_filter"); // Use helper to get alias
                
                // Create a filter operator on top, passing the input_alias
                create_filter(input_op, predicate.clone(), input_alias)
            }
            
            PhysicalPlan::Project { input, columns } => {
                // Build the input operator
                let input_op = self.build_operator_tree(input)?;
                
                // columns is already Vec<String>
                let projection_column_names: Vec<String> = columns.clone();
                
                // Get the alias of the input plan node to ProjectOperator
                let input_alias_str = get_plan_alias(input, "proj_input");

                // Create a projection operator on top
                create_projection(input_op, projection_column_names, input_alias_str)
            }
            
            PhysicalPlan::Materialize { input } => {
                // Simply build the input operator - materialization is handled implicitly
                self.build_operator_tree(input)
            }
            
            PhysicalPlan::NestedLoopJoin { left, right, condition, join_type } => {
                // Build both input operators
                let left_op = self.build_operator_tree(left)?;
                let right_op = self.build_operator_tree(right)?;
                
                // Get aliases for left and right inputs using the helper
                let left_alias_str = get_plan_alias(left, "left");
                let right_alias_str = get_plan_alias(right, "right");
                
                let condition_str = expression_to_predicate(condition);
                let is_left_join = join_type_to_is_left_join(join_type);
                
                create_nested_loop_join(left_op, right_op, condition_str, is_left_join, left_alias_str, right_alias_str)
            }
            
            PhysicalPlan::HashJoin { left, right, condition, join_type } => {
                // Build both input operators
                let left_op = self.build_operator_tree(left)?;
                let right_op = self.build_operator_tree(right)?;
                
                // Get aliases for left and right inputs
                let left_alias = get_plan_alias(left, "left");
                let right_alias = get_plan_alias(right, "right");
                
                // Convert the condition to a string for compatibility
                let condition_str = expression_to_predicate(condition);
                
                // Determine if it's a left join
                let is_left_join = join_type_to_is_left_join(join_type);
                
                // Create a hash join operator
                create_hash_join(left_op, right_op, condition_str, is_left_join, left_alias, right_alias)
            }
            
            PhysicalPlan::HashAggregate { input, group_by, aggregate_expressions, having } => {
                // Build the input operator
                let input_op = self.build_operator_tree(input)?;
                
                // Convert expressions to strings for compatibility with existing operators
                let group_by_strs: Vec<String> = group_by.iter()
                    .map(expression_to_predicate)
                    .collect();
                
                let agg_strs: Vec<String> = aggregate_expressions.iter()
                    .map(expression_to_predicate)
                    .collect();
                
                let having_str = having.as_ref().map(expression_to_predicate);
                
                // Create a hash aggregate operator
                create_hash_aggregate(
                    input_op,
                    group_by_strs,
                    agg_strs,
                    having_str
                )
            }
            
            PhysicalPlan::CreateTable { .. } => {
                // For DDL operations, the ExecutionEngine should handle them directly.
                // This path in OperatorBuilder is for compatibility or if a DDL plan is incorrectly routed.
                Ok(Arc::new(Mutex::new(crate::query::executor::operators::DummyOperator::new())))
            }
            PhysicalPlan::AlterTable { .. } => {
                // Similar to CreateTable, ALTER TABLE should be handled directly by ExecutionEngine.
                // If this path is hit, it indicates a routing issue in the engine for DDL statements.
                Ok(Arc::new(Mutex::new(crate::query::executor::operators::DummyOperator::new())))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::ast::{
        Expression, ColumnReference, Operator as AstOperator, Value, 
        AggregateFunction
    };
    use crate::catalog::{Catalog};
    use std::sync::{Arc, RwLock};
    use crate::storage::buffer::BufferPoolManager;

    // Helper to setup a basic catalog and buffer pool for testing operator builder
    fn create_test_buffer_pool() -> Arc<BufferPoolManager> {
        // Create a temporary file path
        let temp_path = std::env::temp_dir().join("test_db.db");
        let path_str = temp_path.to_str().unwrap().to_string();
        
        // Create buffer pool
        Arc::new(BufferPoolManager::new(100, path_str).unwrap())
    }

    #[test]
    fn test_build_simple_scan() {
        // Create a test buffer pool
        let buffer_pool = create_test_buffer_pool();
        
        // Create a simple table scan plan
        let plan = PhysicalPlan::SeqScan {
            table_name: "users".to_string(),
            alias: None,
        };
        
        // Build the operator tree
        let builder = OperatorBuilder::new(buffer_pool, Arc::new(RwLock::new(Catalog::new())));
        let op_result = builder.build_operator_tree(&plan);
        
        // Check that we successfully built an operator
        assert!(op_result.is_ok());
    }
    
    #[test]
    fn test_build_filter() {
        // Create a test buffer pool
        let buffer_pool = create_test_buffer_pool();
        
        // Create a filter plan
        let plan = PhysicalPlan::Filter {
            input: Box::new(PhysicalPlan::SeqScan {
                table_name: "users".to_string(),
                alias: None,
            }),
            predicate: Expression::BinaryOp {
                left: Box::new(Expression::Column(ColumnReference {
                    name: "id".to_string(),
                    table: None,
                })),
                op: AstOperator::GreaterThan,
                right: Box::new(Expression::Literal(
                    Value::Integer(100)
                )),
            },
        };
        
        // Build the operator tree
        let builder = OperatorBuilder::new(buffer_pool, Arc::new(RwLock::new(Catalog::new())));
        let op_result = builder.build_operator_tree(&plan);
        
        // Check that we successfully built an operator
        assert!(op_result.is_ok());
    }
    
    #[test]
    fn test_join_type_conversion() {
        // Test that join types are correctly converted to boolean flags
        assert!(!join_type_to_is_left_join(&JoinType::Inner));
        assert!(join_type_to_is_left_join(&JoinType::LeftOuter));
    }
    
    #[test]
    fn test_build_complex_tree() {
        // Create a complex plan: SELECT users.name, COUNT(orders.id) 
        // FROM users JOIN orders ON users.id = orders.user_id 
        // WHERE orders.total > 100 
        // GROUP BY users.name
        
        // Base scans
        let users_scan = PhysicalPlan::SeqScan {
            table_name: "users".to_string(),
            alias: None,
        };
        
        let orders_scan = PhysicalPlan::SeqScan {
            table_name: "orders".to_string(),
            alias: None,
        };
        
        // Join condition: users.id = orders.user_id
        let join_condition = Expression::BinaryOp {
            left: Box::new(Expression::Column(ColumnReference {
                name: "id".to_string(),
                table: Some("users".to_string()),
            })),
            op: AstOperator::Equals,
            right: Box::new(Expression::Column(ColumnReference {
                name: "user_id".to_string(),
                table: Some("orders".to_string()),
            })),
        };
        
        // Join plan
        let join_plan = PhysicalPlan::HashJoin {
            left: Box::new(users_scan),
            right: Box::new(orders_scan),
            condition: join_condition,
            join_type: JoinType::Inner,
        };
        
        // Filter: orders.total > 100
        let filter_plan = PhysicalPlan::Filter {
            input: Box::new(join_plan),
            predicate: Expression::BinaryOp {
                left: Box::new(Expression::Column(ColumnReference {
                    name: "total".to_string(),
                    table: Some("orders".to_string()),
                })),
                op: AstOperator::GreaterThan,
                right: Box::new(Expression::Literal(Value::Integer(100))),
            },
        };
        
        // Aggregation
        let agg_plan = PhysicalPlan::HashAggregate {
            input: Box::new(filter_plan),
            group_by: vec![
                Expression::Column(ColumnReference {
                    name: "name".to_string(),
                    table: Some("users".to_string()),
                }),
            ],
            aggregate_expressions: vec![
                Expression::Aggregate {
                    function: AggregateFunction::Count,
                    arg: Some(Box::new(Expression::Column(ColumnReference {
                        name: "id".to_string(),
                        table: Some("orders".to_string()),
                    }))),
                },
            ],
            having: None,
        };
        
        // Projection
        let plan = PhysicalPlan::Project {
            input: Box::new(agg_plan),
            columns: vec!["users.name".to_string(), "COUNT(orders.id)".to_string()],
        };
        
        // Build the operator tree
        let builder = OperatorBuilder::new(create_test_buffer_pool(), Arc::new(RwLock::new(Catalog::new())));
        let op_result = builder.build_operator_tree(&plan);
        
        // Check that we successfully built an operator
        assert!(op_result.is_ok());
    }
    
    #[test]
    fn test_materialization_pass_through() {
        // Create a plan with materialization
        let plan = PhysicalPlan::Materialize {
            input: Box::new(PhysicalPlan::SeqScan {
                table_name: "users".to_string(),
                alias: None,
            }),
        };
        
        // Build the operator tree
        let builder = OperatorBuilder::new(create_test_buffer_pool(), Arc::new(RwLock::new(Catalog::new())));
        let op_result = builder.build_operator_tree(&plan);
        
        // Check that we successfully built an operator
        assert!(op_result.is_ok());
    }
    
    #[test]
    fn test_build_nested_loop_join() {
        // Create a nested loop join plan
        let plan = PhysicalPlan::NestedLoopJoin {
            left: Box::new(PhysicalPlan::SeqScan {
                table_name: "users".to_string(),
                alias: None,
            }),
            right: Box::new(PhysicalPlan::SeqScan {
                table_name: "orders".to_string(),
                alias: None,
            }),
            condition: Expression::BinaryOp {
                left: Box::new(Expression::Column(ColumnReference {
                    name: "id".to_string(),
                    table: Some("users".to_string()),
                })),
                op: AstOperator::Equals,
                right: Box::new(Expression::Column(ColumnReference {
                    name: "user_id".to_string(),
                    table: Some("orders".to_string()),
                })),
            },
            join_type: JoinType::Inner,
        };
        
        // Build the operator tree
        let builder = OperatorBuilder::new(create_test_buffer_pool(), Arc::new(RwLock::new(Catalog::new())));
        let op_result = builder.build_operator_tree(&plan);
        
        // Check that we successfully built an operator
        assert!(op_result.is_ok());
    }
    
    #[test]
    fn test_build_hash_aggregate() {
        // Create an aggregate plan
        let plan = PhysicalPlan::HashAggregate {
            input: Box::new(PhysicalPlan::SeqScan {
                table_name: "orders".to_string(),
                alias: None,
            }),
            group_by: vec![
                Expression::Column(ColumnReference {
                    name: "customer_id".to_string(),
                    table: None,
                }),
            ],
            aggregate_expressions: vec![
                Expression::Aggregate {
                    function: AggregateFunction::Sum,
                    arg: Some(Box::new(Expression::Column(ColumnReference {
                        name: "total".to_string(),
                        table: None,
                    }))),
                },
            ],
            having: Some(Expression::BinaryOp {
                left: Box::new(Expression::Aggregate {
                    function: AggregateFunction::Sum,
                    arg: Some(Box::new(Expression::Column(ColumnReference {
                        name: "total".to_string(),
                        table: None,
                    }))),
                }),
                op: AstOperator::GreaterThan,
                right: Box::new(Expression::Literal(Value::Integer(1000))),
            }),
        };
        
        // Build the operator tree
        let builder = OperatorBuilder::new(create_test_buffer_pool(), Arc::new(RwLock::new(Catalog::new())));
        let op_result = builder.build_operator_tree(&plan);
        
        // Check that we successfully built an operator
        assert!(op_result.is_ok());
    }
    
    #[test]
    fn test_build_create_table() {
        // Create a CREATE TABLE plan
        let plan = PhysicalPlan::CreateTable {
            table_name: "new_table".to_string(),
            columns: vec![],  // Empty for this test
        };
        
        // Build the operator tree
        let builder = OperatorBuilder::new(create_test_buffer_pool(), Arc::new(RwLock::new(Catalog::new())));
        let op_result = builder.build_operator_tree(&plan);
        
        // Check that we successfully built an operator
        assert!(op_result.is_ok());
    }
} 