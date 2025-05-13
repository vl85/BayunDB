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
        PhysicalOptimizer { catalog }
    }
    
    /// Optimize a physical plan
    pub fn optimize(&self, plan: PhysicalPlan) -> PhysicalPlan {
        let plan = self.add_materialization(plan);
        let plan = self.push_down_filters(plan);
        self.optimize_joins(plan)
    }
    
    /// Add materialization nodes where beneficial
    fn add_materialization(&self, plan: PhysicalPlan) -> PhysicalPlan {
        match plan {
            PhysicalPlan::NestedLoopJoin { left, right, condition, join_type } => {
                let optimized_left = self.add_materialization(*left); // Recurse
                let optimized_right = self.add_materialization(*right); // Recurse
                
                let right_with_materialization = match &optimized_right {
                    PhysicalPlan::SeqScan { .. } => optimized_right,
                    PhysicalPlan::Materialize { .. } => optimized_right,
                    _ => PhysicalPlan::Materialize { input: Box::new(optimized_right) },
                };
                
                PhysicalPlan::NestedLoopJoin {
                    left: Box::new(optimized_left),
                    right: Box::new(right_with_materialization),
                    condition,
                    join_type,
                }
            }
            PhysicalPlan::HashJoin { left, right, condition, join_type } => {
                let optimized_left = self.add_materialization(*left); // Recurse
                let optimized_right = self.add_materialization(*right); // Recurse
                
                // Per test_hash_join_not_materialized, HashJoin inputs are not
                // unconditionally materialized by this specific optimization pass.
                PhysicalPlan::HashJoin {
                    left: Box::new(optimized_left),
                    right: Box::new(optimized_right), // Pass through optimized right, no additional materialization here
                    condition,
                    join_type,
                }
            }
            // Recursively apply to inputs of other operators
            PhysicalPlan::Filter { input, predicate } => {
                PhysicalPlan::Filter {
                    input: Box::new(self.add_materialization(*input)),
                    predicate,
                }
            }
            PhysicalPlan::Project { input, columns } => {
                PhysicalPlan::Project {
                    input: Box::new(self.add_materialization(*input)),
                    columns,
                }
            }
            PhysicalPlan::HashAggregate { input, group_by, aggregate_select_expressions, having, output_select_list } => {
                PhysicalPlan::HashAggregate {
                    input: Box::new(self.add_materialization(*input)),
                    group_by,
                    aggregate_select_expressions,
                    having,
                    output_select_list, // Include the field
                }
            }
            PhysicalPlan::Sort { input, order_by } => {
                PhysicalPlan::Sort {
                    input: Box::new(self.add_materialization(*input)),
                    order_by,
                }
            }
            // Base cases: leaf nodes or nodes not modified by this rule
            PhysicalPlan::SeqScan { .. } |
            PhysicalPlan::Materialize { .. } |
            PhysicalPlan::CreateTable { .. } |
            PhysicalPlan::AlterTable { .. } => plan,
        }
    }

    /// Push filter predicates down the plan tree
    fn push_down_filters(&self, plan: PhysicalPlan) -> PhysicalPlan {
        match plan {
            PhysicalPlan::Filter { input, predicate } => {
                // Recursively optimize the input first
                let optimized_input = self.push_down_filters(*input);
                
                // Try to push the current filter down into the optimized input
                self.push_filter_into_node(optimized_input, predicate)
            }
            // Recursively optimize inputs for other nodes
            PhysicalPlan::Project { input, columns } => {
                PhysicalPlan::Project {
                    input: Box::new(self.push_down_filters(*input)),
                    columns,
                }
            }
            PhysicalPlan::NestedLoopJoin { left, right, condition, join_type } => {
                PhysicalPlan::NestedLoopJoin {
                    left: Box::new(self.push_down_filters(*left)),
                    right: Box::new(self.push_down_filters(*right)),
                    condition,
                    join_type,
                }
            }
            PhysicalPlan::HashJoin { left, right, condition, join_type } => {
                PhysicalPlan::HashJoin {
                    left: Box::new(self.push_down_filters(*left)),
                    right: Box::new(self.push_down_filters(*right)),
                    condition,
                    join_type,
                }
            }
            PhysicalPlan::HashAggregate { input, group_by, aggregate_select_expressions, having, output_select_list } => {
                PhysicalPlan::HashAggregate {
                    input: Box::new(self.push_down_filters(*input)),
                    group_by,
                    aggregate_select_expressions,
                    having,
                    output_select_list, // Include the field
                }
            }
            PhysicalPlan::Sort { input, order_by } => {
                PhysicalPlan::Sort {
                    input: Box::new(self.push_down_filters(*input)),
                    order_by,
                }
            }
            // Base cases: leaf nodes or nodes not modified by this rule
            PhysicalPlan::SeqScan { .. } |
            PhysicalPlan::Materialize { .. } |
            PhysicalPlan::CreateTable { .. } |
            PhysicalPlan::AlterTable { .. } => plan,
        }
    }

    fn get_plan_source_table_aliases_recursive(&self, plan: &PhysicalPlan, aliases: &mut HashSet<String>) {
        match plan {
            PhysicalPlan::SeqScan { table_name, alias, .. } => {
                if let Some(a) = alias {
                    aliases.insert(a.clone());
                } else {
                    aliases.insert(table_name.clone());
                }
            }
            PhysicalPlan::Project { input, .. } |
            PhysicalPlan::Filter { input, .. } |
            PhysicalPlan::Sort { input, .. } |
            PhysicalPlan::HashAggregate { input, .. } |
            PhysicalPlan::Materialize { input, .. } => {
                self.get_plan_source_table_aliases_recursive(input, aliases);
            }
            PhysicalPlan::NestedLoopJoin { left, right, .. } |
            PhysicalPlan::HashJoin { left, right, .. } => {
                self.get_plan_source_table_aliases_recursive(left, aliases);
                self.get_plan_source_table_aliases_recursive(right, aliases);
            }
            PhysicalPlan::CreateTable { .. } | PhysicalPlan::AlterTable { .. } => {
                // No source table aliases from DDL plans in this context
            }
        }
    }

    fn get_plan_source_table_aliases(&self, plan: &PhysicalPlan) -> HashSet<String> {
        let mut aliases = HashSet::new();
        self.get_plan_source_table_aliases_recursive(plan, &mut aliases);
        aliases
    }

    // Helper to push a filter predicate into a node
    fn push_filter_into_node(&self, node: PhysicalPlan, predicate: Expression) -> PhysicalPlan {
        match node {
            // Push filter into another filter (combine predicates)
            PhysicalPlan::Filter { input, predicate: existing_predicate } => {
                let combined_predicate = Expression::BinaryOp {
                    left: Box::new(predicate),
                    op: AstOperator::And,
                    right: Box::new(existing_predicate),
                };
                // Recursively try to push the combined predicate further down
                self.push_filter_into_node(*input, combined_predicate)
            }
            // Push filter below project if possible (predicate only uses columns available below project)
            PhysicalPlan::Project { input, columns } => {
                // Simplistic check: Assume filter can be pushed if Project doesn't involve complex expressions yet.
                // A proper implementation needs schema analysis.
                let pushed_input = self.push_filter_into_node(*input, predicate);
                PhysicalPlan::Project { input: Box::new(pushed_input), columns }
            }
            
            PhysicalPlan::NestedLoopJoin { left, right, condition, join_type } => {
                let mut left_filters = Vec::new();
                let mut right_filters = Vec::new();
                let mut remaining_predicates = Vec::new();

                let mut conjuncts = Vec::new();
                split_conjunction_recursive(&predicate, &mut conjuncts);

                let left_child_aliases = self.get_plan_source_table_aliases(&left);
                let right_child_aliases = self.get_plan_source_table_aliases(&right);

                for p_conjunct in conjuncts {
                    let pred_cols = extract_column_identifiers(&p_conjunct);
                    if pred_cols.is_empty() {
                        remaining_predicates.push(p_conjunct);
                        continue;
                    }

                    let mut refers_to_left = false;
                    let mut refers_to_right = false;
                    let mut refers_to_other = false;

                    for col_ident_str in &pred_cols {
                        let mut matched_to_known_alias = false;
                        if let Some(alias_candidate) = col_ident_str.split('.').next() {
                            if left_child_aliases.contains(alias_candidate) {
                                refers_to_left = true;
                                matched_to_known_alias = true;
                            }
                            if right_child_aliases.contains(alias_candidate) {
                                refers_to_right = true;
                                matched_to_known_alias = true;
                            }
                            if !matched_to_known_alias && col_ident_str.contains('.') {
                                refers_to_other = true;
                            }
                        }
                        if !col_ident_str.contains('.') {
                             refers_to_other = true;
                        }
                    }
                    
                    if refers_to_other || (refers_to_left && refers_to_right) {
                        remaining_predicates.push(p_conjunct);
                    } else if refers_to_left {
                        left_filters.push(p_conjunct);
                    } else if refers_to_right {
                        right_filters.push(p_conjunct);
                    } else {
                        remaining_predicates.push(p_conjunct);
                    }
                }

                let mut new_left = *left;
                if !left_filters.is_empty() {
                    if let Some(left_predicate) = build_expression_from_conjunctions(left_filters) {
                        new_left = self.push_filter_into_node(new_left, left_predicate);
                    }
                }

                let mut new_right = *right;
                if !right_filters.is_empty() {
                    if let Some(right_predicate) = build_expression_from_conjunctions(right_filters) {
                        new_right = self.push_filter_into_node(new_right, right_predicate);
                    }
                }
                
                let new_join_plan = PhysicalPlan::NestedLoopJoin {
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                    condition, // Consumed and moved
                    join_type, // Consumed and moved
                };

                if !remaining_predicates.is_empty() {
                    if let Some(final_predicate) = build_expression_from_conjunctions(remaining_predicates) {
                        PhysicalPlan::Filter { input: Box::new(new_join_plan), predicate: final_predicate }
                    } else {
                        new_join_plan
                    }
                } else {
                    new_join_plan
                }
            }
            PhysicalPlan::HashJoin { left, right, condition, join_type } => {
                let mut left_filters = Vec::new();
                let mut right_filters = Vec::new();
                let mut remaining_predicates = Vec::new();

                let mut conjuncts = Vec::new();
                split_conjunction_recursive(&predicate, &mut conjuncts);

                let left_child_aliases = self.get_plan_source_table_aliases(&left);
                let right_child_aliases = self.get_plan_source_table_aliases(&right);

                for p_conjunct in conjuncts {
                    let pred_cols = extract_column_identifiers(&p_conjunct);
                    if pred_cols.is_empty() {
                        remaining_predicates.push(p_conjunct);
                        continue;
                    }

                    let mut refers_to_left = false;
                    let mut refers_to_right = false;
                    let mut refers_to_other = false;

                    for col_ident_str in &pred_cols {
                        let mut matched_to_known_alias = false;
                        if let Some(alias_candidate) = col_ident_str.split('.').next() {
                            if left_child_aliases.contains(alias_candidate) {
                                refers_to_left = true;
                                matched_to_known_alias = true;
                            }
                            if right_child_aliases.contains(alias_candidate) {
                                refers_to_right = true;
                                matched_to_known_alias = true;
                            }
                            if !matched_to_known_alias && col_ident_str.contains('.') {
                                refers_to_other = true;
                            }
                        }
                        if !col_ident_str.contains('.') {
                             refers_to_other = true;
                        }
                    }
                    
                    if refers_to_other || (refers_to_left && refers_to_right) {
                        remaining_predicates.push(p_conjunct);
                    } else if refers_to_left {
                        left_filters.push(p_conjunct);
                    } else if refers_to_right {
                        right_filters.push(p_conjunct);
                    } else {
                        remaining_predicates.push(p_conjunct);
                    }
                }

                let mut new_left = *left;
                if !left_filters.is_empty() {
                    if let Some(left_predicate) = build_expression_from_conjunctions(left_filters) {
                        new_left = self.push_filter_into_node(new_left, left_predicate);
                    }
                }

                let mut new_right = *right;
                if !right_filters.is_empty() {
                    if let Some(right_predicate) = build_expression_from_conjunctions(right_filters) {
                        new_right = self.push_filter_into_node(new_right, right_predicate);
                    }
                }
                
                let new_join_plan = PhysicalPlan::HashJoin {
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                    condition, // Consumed and moved
                    join_type, // Consumed and moved
                };

                if !remaining_predicates.is_empty() {
                    if let Some(final_predicate) = build_expression_from_conjunctions(remaining_predicates) {
                        PhysicalPlan::Filter { input: Box::new(new_join_plan), predicate: final_predicate }
                    } else {
                        new_join_plan
                    }
                } else {
                    new_join_plan
                }
            }
            // Cannot push filter below aggregation or sort easily
            PhysicalPlan::HashAggregate { .. } | PhysicalPlan::Sort { .. } => {
                PhysicalPlan::Filter { input: Box::new(node), predicate }
            }
            // Push filter onto scan
            PhysicalPlan::SeqScan { .. } => {
                PhysicalPlan::Filter { input: Box::new(node), predicate }
            }
            // Cannot push filter below these
            PhysicalPlan::Materialize { .. } |
            PhysicalPlan::CreateTable { .. } |
            PhysicalPlan::AlterTable { .. } => {
                 PhysicalPlan::Filter { input: Box::new(node), predicate }
            }
        }
    }

    /// Optimize join strategies (e.g., choose hash join over nested loop)
    fn optimize_joins(&self, plan: PhysicalPlan) -> PhysicalPlan {
        // TODO: Implement join optimization logic (e.g., cost-based selection)
        // For now, just pass through
        plan
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