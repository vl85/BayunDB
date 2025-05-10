// Physical Optimizer
//
// This module provides optimization logic for physical query plans.

use crate::query::planner::physical_plan::PhysicalPlan;

/// Physical optimizer for query plan optimization
pub struct PhysicalOptimizer;

impl PhysicalOptimizer {
    /// Create a new physical optimizer
    pub fn new() -> Self {
        PhysicalOptimizer
    }
    
    /// Optimize a physical plan
    pub fn optimize(&self, plan: PhysicalPlan) -> PhysicalPlan {
        // Apply a series of optimization rules
        let plan = self.add_materialization(plan);
        let plan = self.optimize_joins(plan);
        
        plan
    }
    
    /// Add materialization nodes where beneficial
    fn add_materialization(&self, plan: PhysicalPlan) -> PhysicalPlan {
        match plan {
            // For join operations, materialize the inner (right) side if it's not a simple scan
            PhysicalPlan::NestedLoopJoin { left, right, condition, join_type } => {
                let optimized_left = self.optimize((*left).clone());
                let optimized_right = self.optimize((*right).clone());
                
                // Decide if we need to materialize the right side
                let right_with_materialization = match optimized_right {
                    // Don't materialize simple scans
                    PhysicalPlan::SeqScan { .. } => optimized_right,
                    // Already materialized
                    PhysicalPlan::Materialize { .. } => optimized_right,
                    // Add materialization for complex operations
                    _ => PhysicalPlan::Materialize {
                        input: Box::new(optimized_right),
                    }
                };
                
                PhysicalPlan::NestedLoopJoin {
                    left: Box::new(optimized_left),
                    right: Box::new(right_with_materialization),
                    condition,
                    join_type,
                }
            }
            PhysicalPlan::HashJoin { left, right, condition, join_type } => {
                let optimized_left = self.optimize((*left).clone());
                let optimized_right = self.optimize((*right).clone());
                
                PhysicalPlan::HashJoin {
                    left: Box::new(optimized_left),
                    right: Box::new(optimized_right),
                    condition,
                    join_type,
                }
            }
            // Recursively optimize other plan types
            PhysicalPlan::Filter { input, predicate } => {
                PhysicalPlan::Filter {
                    input: Box::new(self.optimize((*input).clone())),
                    predicate,
                }
            }
            PhysicalPlan::Project { input, columns } => {
                PhysicalPlan::Project {
                    input: Box::new(self.optimize((*input).clone())),
                    columns,
                }
            }
            PhysicalPlan::Materialize { input } => {
                PhysicalPlan::Materialize {
                    input: Box::new(self.optimize((*input).clone())),
                }
            }
            PhysicalPlan::HashAggregate { input, group_by, aggregate_expressions, having } => {
                PhysicalPlan::HashAggregate {
                    input: Box::new(self.optimize((*input).clone())),
                    group_by,
                    aggregate_expressions,
                    having,
                }
            }
            // For leaf nodes, return as-is
            PhysicalPlan::SeqScan { .. } => plan,
            PhysicalPlan::CreateTable { .. } => plan,
            PhysicalPlan::AlterTable { .. } => plan,
        }
    }
    
    /// Optimize join operations
    fn optimize_joins(&self, plan: PhysicalPlan) -> PhysicalPlan {
        // This is a placeholder for join optimization logic
        // In a real implementation, we might:
        // - Reorder joins based on cardinality estimates
        // - Choose between nested loop and hash join based on join attributes
        plan
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::ast::{Expression, Operator, ColumnReference, Value, JoinType};
    
    #[test]
    fn test_materialize_nested_loop_join() {
        // Create a nested loop join with a complex right side
        let plan = PhysicalPlan::NestedLoopJoin {
            left: Box::new(PhysicalPlan::SeqScan {
                table_name: "users".to_string(),
                alias: None,
            }),
            right: Box::new(PhysicalPlan::Filter {
                input: Box::new(PhysicalPlan::SeqScan {
                    table_name: "orders".to_string(),
                    alias: None,
                }),
                predicate: Expression::BinaryOp {
                    left: Box::new(Expression::Column(ColumnReference {
                        name: "user_id".to_string(),
                        table: None,
                    })),
                    op: Operator::Equals,
                    right: Box::new(Expression::Literal(
                        Value::Integer(1)
                    )),
                },
            }),
            condition: Expression::BinaryOp {
                left: Box::new(Expression::Column(ColumnReference {
                    name: "id".to_string(),
                    table: Some("users".to_string()),
                })),
                op: Operator::Equals,
                right: Box::new(Expression::Column(ColumnReference {
                    name: "user_id".to_string(),
                    table: Some("orders".to_string()),
                })),
            },
            join_type: JoinType::Inner,
        };
        
        // Apply optimization
        let optimizer = PhysicalOptimizer::new();
        let optimized = optimizer.optimize(plan);
        
        // Check if right side is now materialized
        if let PhysicalPlan::NestedLoopJoin { right, .. } = optimized {
            if let PhysicalPlan::Materialize { .. } = *right {
                // Successfully materialized
            } else {
                panic!("Right side of join was not materialized");
            }
        } else {
            panic!("Optimized plan is not a join");
        }
    }
    
    #[test]
    fn test_no_materialize_simple_scan() {
        // Create a nested loop join with a simple scan on the right side
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
                op: Operator::Equals,
                right: Box::new(Expression::Column(ColumnReference {
                    name: "user_id".to_string(),
                    table: Some("orders".to_string()),
                })),
            },
            join_type: JoinType::Inner,
        };
        
        // Apply optimization
        let optimizer = PhysicalOptimizer::new();
        let optimized = optimizer.optimize(plan);
        
        // Check that the right side is NOT materialized since it's a simple scan
        if let PhysicalPlan::NestedLoopJoin { right, .. } = optimized {
            if let PhysicalPlan::SeqScan { .. } = *right {
                // Correctly not materialized
            } else {
                panic!("Right side of join was unnecessarily materialized");
            }
        } else {
            panic!("Optimized plan is not a join");
        }
    }
    
    #[test]
    fn test_already_materialized() {
        // Create a nested loop join with an already materialized right side
        let plan = PhysicalPlan::NestedLoopJoin {
            left: Box::new(PhysicalPlan::SeqScan {
                table_name: "users".to_string(),
                alias: None,
            }),
            right: Box::new(PhysicalPlan::Materialize {
                input: Box::new(PhysicalPlan::Filter {
                    input: Box::new(PhysicalPlan::SeqScan {
                        table_name: "orders".to_string(),
                        alias: None,
                    }),
                    predicate: Expression::BinaryOp {
                        left: Box::new(Expression::Column(ColumnReference {
                            name: "user_id".to_string(),
                            table: None,
                        })),
                        op: Operator::Equals,
                        right: Box::new(Expression::Literal(
                            Value::Integer(1)
                        )),
                    },
                }),
            }),
            condition: Expression::BinaryOp {
                left: Box::new(Expression::Column(ColumnReference {
                    name: "id".to_string(),
                    table: Some("users".to_string()),
                })),
                op: Operator::Equals,
                right: Box::new(Expression::Column(ColumnReference {
                    name: "user_id".to_string(),
                    table: Some("orders".to_string()),
                })),
            },
            join_type: JoinType::Inner,
        };
        
        // Apply optimization
        let optimizer = PhysicalOptimizer::new();
        let optimized = optimizer.optimize(plan);
        
        // Check that the right side remains materialized but doesn't get double-materialized
        if let PhysicalPlan::NestedLoopJoin { right, .. } = optimized {
            if let PhysicalPlan::Materialize { input } = &*right {
                // Check that the input to materialization is not another materialization
                if let PhysicalPlan::Materialize { .. } = **input {
                    panic!("Right side of join was double-materialized");
                }
                // Correctly kept the existing materialization
            } else {
                panic!("Right side of join lost its materialization");
            }
        } else {
            panic!("Optimized plan is not a join");
        }
    }
    
    #[test]
    fn test_recursive_optimization() {
        // Create a deeply nested plan
        let deepest_plan = PhysicalPlan::Filter {
            input: Box::new(PhysicalPlan::SeqScan {
                table_name: "products".to_string(),
                alias: None,
            }),
            predicate: Expression::BinaryOp {
                left: Box::new(Expression::Column(ColumnReference {
                    name: "price".to_string(),
                    table: None,
                })),
                op: Operator::GreaterThan,
                right: Box::new(Expression::Literal(
                    Value::Integer(100)
                )),
            },
        };
        
        let middle_plan = PhysicalPlan::NestedLoopJoin {
            left: Box::new(PhysicalPlan::SeqScan {
                table_name: "orders".to_string(),
                alias: None,
            }),
            right: Box::new(deepest_plan),
            condition: Expression::BinaryOp {
                left: Box::new(Expression::Column(ColumnReference {
                    name: "product_id".to_string(),
                    table: Some("orders".to_string()),
                })),
                op: Operator::Equals,
                right: Box::new(Expression::Column(ColumnReference {
                    name: "id".to_string(),
                    table: Some("products".to_string()),
                })),
            },
            join_type: JoinType::Inner,
        };
        
        let top_plan = PhysicalPlan::NestedLoopJoin {
            left: Box::new(PhysicalPlan::SeqScan {
                table_name: "users".to_string(),
                alias: None,
            }),
            right: Box::new(middle_plan),
            condition: Expression::BinaryOp {
                left: Box::new(Expression::Column(ColumnReference {
                    name: "id".to_string(),
                    table: Some("users".to_string()),
                })),
                op: Operator::Equals,
                right: Box::new(Expression::Column(ColumnReference {
                    name: "user_id".to_string(),
                    table: Some("orders".to_string()),
                })),
            },
            join_type: JoinType::Inner,
        };
        
        // Apply optimization
        let optimizer = PhysicalOptimizer::new();
        let optimized = optimizer.optimize(top_plan);
        
        // Verify that optimization was applied recursively
        // Top join should have materialized its right side (middle_plan)
        if let PhysicalPlan::NestedLoopJoin { right: top_right, .. } = optimized {
            if let PhysicalPlan::Materialize { input: materialized_middle } = &*top_right {
                // The materialized middle plan should be a nested loop join
                if let PhysicalPlan::NestedLoopJoin { right: middle_right, .. } = &**materialized_middle {
                    // The right side of the middle join should be materialized (deepest_plan)
                    if let PhysicalPlan::Materialize { .. } = &**middle_right {
                        // Successfully applied recursive optimization
                    } else {
                        panic!("Deepest level not materialized");
                    }
                } else {
                    panic!("Middle level not a join after optimization");
                }
            } else {
                panic!("Middle level not materialized");
            }
        } else {
            panic!("Top level not a join after optimization");
        }
    }
    
    #[test]
    fn test_hash_join_not_materialized() {
        // Create a hash join with a complex right side
        // Hash joins don't need to materialize their inputs in the current optimizer
        let plan = PhysicalPlan::HashJoin {
            left: Box::new(PhysicalPlan::SeqScan {
                table_name: "users".to_string(),
                alias: None,
            }),
            right: Box::new(PhysicalPlan::Filter {
                input: Box::new(PhysicalPlan::SeqScan {
                    table_name: "orders".to_string(),
                    alias: None,
                }),
                predicate: Expression::BinaryOp {
                    left: Box::new(Expression::Column(ColumnReference {
                        name: "user_id".to_string(),
                        table: None,
                    })),
                    op: Operator::Equals,
                    right: Box::new(Expression::Literal(
                        Value::Integer(1)
                    )),
                },
            }),
            condition: Expression::BinaryOp {
                left: Box::new(Expression::Column(ColumnReference {
                    name: "id".to_string(),
                    table: Some("users".to_string()),
                })),
                op: Operator::Equals,
                right: Box::new(Expression::Column(ColumnReference {
                    name: "user_id".to_string(),
                    table: Some("orders".to_string()),
                })),
            },
            join_type: JoinType::Inner,
        };
        
        // Apply optimization
        let optimizer = PhysicalOptimizer::new();
        let optimized = optimizer.optimize(plan);
        
        // Verify that hash join inputs aren't materialized
        if let PhysicalPlan::HashJoin { right, .. } = optimized {
            match *right {
                PhysicalPlan::Filter { .. } => {
                    // Filter wasn't materialized, as expected
                },
                PhysicalPlan::Materialize { .. } => {
                    panic!("Hash join input was unnecessarily materialized");
                },
                _ => {
                    panic!("Right side of hash join was unexpected type");
                }
            }
        } else {
            panic!("Optimized plan is not a hash join");
        }
    }
} 