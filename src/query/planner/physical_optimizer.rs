// Physical Optimizer
//
// This module provides optimization logic for physical query plans.

use crate::query::planner::physical_plan::PhysicalPlan;
use crate::query::parser::ast::{Expression, Operator as AstOperator};
use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use crate::catalog::Catalog;

use crate::query::planner::filter_pushdown::FilterPushdownOptimizer;
use crate::query::planner::materialization::MaterializationOptimizer;
use crate::query::planner::join_reordering::JoinReorderingOptimizer;

/// Extracts all unique column identifiers (table.column or column) from an expression.
pub fn extract_column_identifiers(expr: &Expression) -> HashSet<String> {
    let mut identifiers = HashSet::new();
    match expr {
        Expression::Column(col_ref) => {
            if let Some(table) = &col_ref.table {
                identifiers.insert(format!("{}.{}", table, col_ref.name));
            } else {
                identifiers.insert(col_ref.name.clone());
            }
        }
        Expression::BinaryOp { left, right, .. } => {
            identifiers.extend(extract_column_identifiers(left));
            identifiers.extend(extract_column_identifiers(right));
        }
        Expression::UnaryOp { expr: sub_expr, .. } => {
            identifiers.extend(extract_column_identifiers(sub_expr));
        }
        Expression::Function { args, .. } => {
            for arg in args {
                identifiers.extend(extract_column_identifiers(arg));
            }
        }
        Expression::Aggregate { arg, .. } => {
            if let Some(arg_expr) = arg {
                identifiers.extend(extract_column_identifiers(arg_expr));
            }
        }
        Expression::Case { operand, when_then_clauses, else_clause } => {
            if let Some(op_expr) = operand {
                identifiers.extend(extract_column_identifiers(op_expr));
            }
            for (when_expr, then_expr) in when_then_clauses {
                identifiers.extend(extract_column_identifiers(when_expr));
                identifiers.extend(extract_column_identifiers(then_expr));
            }
            if let Some(el_expr) = else_clause {
                identifiers.extend(extract_column_identifiers(el_expr));
            }
        }
        Expression::IsNull { expr: sub_expr, .. } => {
            identifiers.extend(extract_column_identifiers(sub_expr));
        }
        Expression::Literal(_) => { /* No columns in literals */ }
    }
    identifiers
}

/// Splits a predicate into a list of conjoined expressions.
/// e.g., A AND B AND C becomes [A, B, C]
pub fn split_conjunction_recursive(predicate: &Expression, conditions: &mut Vec<Expression>) {
    match predicate {
        Expression::BinaryOp { left, op: AstOperator::And, right } => {
            split_conjunction_recursive(left, conditions);
            split_conjunction_recursive(right, conditions);
        }
        _ => {
            conditions.push(predicate.clone());
        }
    }
}

pub fn build_expression_from_conjunctions(mut conditions: Vec<Expression>) -> Option<Expression> {
    if conditions.is_empty() {
        None
    } else {
        let mut current_expr = conditions.pop().unwrap();
        while let Some(next_expr) = conditions.pop() {
            current_expr = Expression::BinaryOp {
                left: Box::new(next_expr),
                op: AstOperator::And,
                right: Box::new(current_expr),
            };
        }
        Some(current_expr)
    }
}

/// Physical optimizer for query plan optimization
pub struct PhysicalOptimizer {
    catalog: Arc<RwLock<Catalog>>,
    filter_pushdown: FilterPushdownOptimizer,
    materialization: MaterializationOptimizer,
    join_reordering: JoinReorderingOptimizer,
}

impl PhysicalOptimizer {
    /// Create a new physical optimizer
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        let filter_pushdown = FilterPushdownOptimizer::new(catalog.clone());
        let materialization = MaterializationOptimizer::new(catalog.clone());
        let join_reordering = JoinReorderingOptimizer::new(catalog.clone());
        
        PhysicalOptimizer { 
            catalog,
            filter_pushdown,
            materialization,
            join_reordering,
        }
    }
    
    /// Optimize a physical plan
    pub fn optimize(&self, plan: PhysicalPlan) -> PhysicalPlan {
        // Apply optimizations in sequence
        let materialized_plan = self.materialization.optimize(plan);
        let filtered_plan = self.filter_pushdown.optimize(materialized_plan);
        self.join_reordering.optimize(filtered_plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::ast::{Expression, Operator, ColumnReference, Value, JoinType};
    use crate::catalog::Catalog;
    use std::sync::{Arc, RwLock};

    #[test]
    fn test_materialize_nested_loop_join() {
        let catalog = Arc::new(RwLock::new(Catalog::new()));
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
        let optimizer = PhysicalOptimizer::new(catalog.clone());
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
        let catalog = Arc::new(RwLock::new(Catalog::new()));
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
        let optimizer = PhysicalOptimizer::new(catalog.clone());
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
        let catalog = Arc::new(RwLock::new(Catalog::new()));
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
        let optimizer = PhysicalOptimizer::new(catalog.clone());
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
        let catalog = Arc::new(RwLock::new(Catalog::new()));
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
        let optimizer = PhysicalOptimizer::new(catalog.clone());
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
        let catalog = Arc::new(RwLock::new(Catalog::new()));
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
        let optimizer = PhysicalOptimizer::new(catalog.clone());
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