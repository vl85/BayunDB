// Operator Builder
//
// This module builds executable operator trees from physical plans.

use std::sync::{Arc, Mutex, RwLock};

use crate::query::executor::operators; // Import the module
use crate::query::executor::operators::Operator; // Import the trait specifically
use crate::query::executor::result::QueryResult;
use crate::query::planner::physical_plan::PhysicalPlan;
use crate::query::parser::ast::JoinType;
use crate::storage::buffer::BufferPoolManager;
use crate::catalog::Catalog;
use crate::query::parser::ast::Expression;
use crate::query::parser::ast::ColumnReference;

/// Converts a JoinType to the is_left_join boolean flag expected by operators
fn join_type_to_is_left_join(join_type: &JoinType) -> bool {
    match join_type {
        JoinType::LeftOuter => true,
        _ => false
    }
}

/// Helper function to get a default alias for a plan node
fn get_plan_alias(plan: &PhysicalPlan, default_prefix: &str) -> String {
    match plan {
        PhysicalPlan::SeqScan { alias, table_name } => alias.clone().unwrap_or_else(|| table_name.clone()),
        PhysicalPlan::Filter { input, .. } => get_plan_alias(input, default_prefix),
        PhysicalPlan::Project { input, .. } => get_plan_alias(input, default_prefix),
        PhysicalPlan::HashJoin { .. } => format!("{}_join", default_prefix), // Join output schema is complex
        PhysicalPlan::NestedLoopJoin { .. } => format!("{}_join", default_prefix),
        PhysicalPlan::HashAggregate { .. } => format!("{}_agg", default_prefix),
        PhysicalPlan::Sort { input, .. } => get_plan_alias(input, default_prefix),
        _ => default_prefix.to_string(), // Default for others
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
    pub fn build_operator_tree(&self, plan: &PhysicalPlan) -> QueryResult<Arc<Mutex<dyn Operator + Send + Sync>>> {
        match plan {
            PhysicalPlan::SeqScan { table_name, alias } => {
                let operator_alias = alias.clone().unwrap_or_else(|| table_name.clone());
                operators::scan::create_table_scan(table_name, &operator_alias, self.buffer_pool.clone(), self.catalog.clone())
            }
            
            PhysicalPlan::Filter { input, predicate } => {
                let input_op = self.build_operator_tree(input)?;
                let input_alias = get_plan_alias(input, "filter_input");
                operators::create_filter(input_op, predicate.clone(), input_alias)
            }
            
            PhysicalPlan::Project { input, columns: proj_columns } => {
                let input_op = self.build_operator_tree(input)?;
                // The Project physical plan stores `Vec<String>` which are just output names.
                // The ProjectionOperator needs `Vec<(Expression, String)>` where the Expression
                // is usually a ColumnReference matching the output name, unless it's aliased.
                // For now, assume simple projection where output name = column name.
                // TODO: Handle aliases properly when Project plan includes expressions.
                let expressions_with_names: Vec<(Expression, String)> = proj_columns
                    .iter()
                    .map(|col_name| (Expression::Column(ColumnReference { table: None, name: col_name.clone() }), col_name.clone()))
                    .collect();

                // Call ProjectionOperator::new with the correct Vec<(Expression, String)>
                Ok(Arc::new(Mutex::new(operators::project::ProjectionOperator::new(
                    input_op,
                    expressions_with_names,
                    // input_alias, // Removed as ProjectionOperator::new only takes 2 args
                ))))
            }
            
            PhysicalPlan::Materialize { input } => {
                // Materialize might just pass through or involve a specific operator
                // For now, just recurse.
                self.build_operator_tree(input)
            }
            
            PhysicalPlan::NestedLoopJoin { left, right, condition, join_type } => {
                let left_op = self.build_operator_tree(left)?;
                let right_op = self.build_operator_tree(right)?;
                let left_alias = get_plan_alias(left, "left");
                let right_alias = get_plan_alias(right, "right");
                let condition_str = crate::query::planner::physical_plan::expression_to_predicate(condition);
                let is_left_join = matches!(join_type, crate::query::parser::ast::JoinType::LeftOuter);
                operators::join::create_nested_loop_join(left_op, right_op, condition_str, is_left_join, left_alias, right_alias)
            }
            
            PhysicalPlan::HashJoin { left, right, condition, join_type } => {
                 let left_op = self.build_operator_tree(left)?;
                 let right_op = self.build_operator_tree(right)?;
                 let left_alias = get_plan_alias(left, "left");
                 let right_alias = get_plan_alias(right, "right");
                 let condition_str = crate::query::planner::physical_plan::expression_to_predicate(condition);
                 let is_left_join = matches!(join_type, crate::query::parser::ast::JoinType::LeftOuter);
                 operators::join::create_hash_join(left_op, right_op, condition_str, is_left_join, left_alias, right_alias)
            }
            
            PhysicalPlan::HashAggregate { 
                input, 
                group_by, 
                aggregate_select_expressions, 
                having, 
                output_select_list
            } => {
                let input_op = self.build_operator_tree(input)?;
                operators::agg::create_hash_aggregate(
                    input_op,
                    group_by.clone(),
                    aggregate_select_expressions.clone(),
                    having.clone(),
                    output_select_list.clone()
                )
            }
            
            PhysicalPlan::CreateTable { .. } | PhysicalPlan::AlterTable { .. } => {
                // DDL operations handled by ExecutionEngine, not converted to operators here.
                Ok(Arc::new(Mutex::new(crate::query::executor::operators::DummyOperator::new())))
            }
            PhysicalPlan::Sort { input, order_by } => {
                let input_op = self.build_operator_tree(input)?;
                operators::sort::create_sort_operator(input_op, order_by.clone())
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
    use crate::query::planner::physical_plan::PhysicalSelectExpression;

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
        let user_name_col = Expression::Column(ColumnReference {
            name: "name".to_string(),
            table: Some("users".to_string()),
        });
        let count_orders_agg = Expression::Aggregate {
            function: AggregateFunction::Count,
            arg: Some(Box::new(Expression::Column(ColumnReference {
                name: "id".to_string(),
                table: Some("orders".to_string()),
            }))),
        };

        let agg_plan = PhysicalPlan::HashAggregate {
            input: Box::new(filter_plan),
            group_by: vec![user_name_col.clone()],
            aggregate_select_expressions: vec![
                PhysicalSelectExpression {
                    expression: user_name_col.clone(),
                    output_name: "user_name".to_string(),
                },
                PhysicalSelectExpression {
                    expression: count_orders_agg.clone(),
                    output_name: "order_count".to_string(),
                }
            ],
            having: None,
            output_select_list: vec![],
        };
        
        // Projection (often omitted after Aggregate if Aggregate outputs the final schema)
        // For testing builder, let's assume Projection is still needed sometimes
        let plan = PhysicalPlan::Project {
            input: Box::new(agg_plan),
            columns: vec!["user_name".to_string(), "order_count".to_string()],
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
        let sum_total_agg = Expression::Aggregate {
            function: AggregateFunction::Sum,
            arg: Some(Box::new(Expression::Column(ColumnReference {
                name: "total".to_string(),
                table: None,
            }))),
        };
        let customer_id_col = Expression::Column(ColumnReference {
            name: "customer_id".to_string(),
            table: None,
        });

        // Create an aggregate plan
        let plan = PhysicalPlan::HashAggregate {
            input: Box::new(PhysicalPlan::SeqScan {
                table_name: "orders".to_string(),
                alias: None,
            }),
            group_by: vec![customer_id_col.clone()],
            aggregate_select_expressions: vec![
                PhysicalSelectExpression {
                    expression: customer_id_col.clone(),
                    output_name: "customer_id".to_string(),
                },
                PhysicalSelectExpression {
                    expression: sum_total_agg.clone(),
                    output_name: "total_sum".to_string(),
                }
            ],
            having: Some(Expression::BinaryOp {
                left: Box::new(sum_total_agg.clone()),
                op: AstOperator::GreaterThan,
                right: Box::new(Expression::Literal(Value::Integer(1000))),
            }),
            output_select_list: vec![],
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