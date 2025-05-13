// Filter Pushdown Optimizer
//
// This module provides filter pushdown optimization logic for physical query plans.

use crate::query::planner::physical_plan::PhysicalPlan;
use crate::query::parser::ast::{Expression, Operator as AstOperator};
use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use crate::catalog::Catalog;

/// Filter Pushdown Optimizer component
pub struct FilterPushdownOptimizer {
    catalog: Arc<RwLock<Catalog>>,
}

impl FilterPushdownOptimizer {
    /// Create a new filter pushdown optimizer
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        FilterPushdownOptimizer { catalog }
    }
    
    /// Apply filter pushdown optimization to a physical plan
    pub fn optimize(&self, plan: PhysicalPlan) -> PhysicalPlan {
        match plan {
            PhysicalPlan::Filter { input, predicate } => {
                // Recursively optimize the input first
                let optimized_input = self.optimize(*input);
                
                // Try to push the current filter down into the optimized input
                self.push_filter_into_node(optimized_input, predicate)
            }
            // Recursively optimize inputs for other nodes
            PhysicalPlan::Project { input, columns } => {
                PhysicalPlan::Project {
                    input: Box::new(self.optimize(*input)),
                    columns,
                }
            }
            PhysicalPlan::NestedLoopJoin { left, right, condition, join_type } => {
                PhysicalPlan::NestedLoopJoin {
                    left: Box::new(self.optimize(*left)),
                    right: Box::new(self.optimize(*right)),
                    condition,
                    join_type,
                }
            }
            PhysicalPlan::HashJoin { left, right, condition, join_type } => {
                PhysicalPlan::HashJoin {
                    left: Box::new(self.optimize(*left)),
                    right: Box::new(self.optimize(*right)),
                    condition,
                    join_type,
                }
            }
            PhysicalPlan::HashAggregate { input, group_by, aggregate_select_expressions, having, output_select_list } => {
                PhysicalPlan::HashAggregate {
                    input: Box::new(self.optimize(*input)),
                    group_by,
                    aggregate_select_expressions,
                    having,
                    output_select_list,
                }
            }
            PhysicalPlan::Sort { input, order_by } => {
                PhysicalPlan::Sort {
                    input: Box::new(self.optimize(*input)),
                    order_by,
                }
            }
            // Base cases: leaf nodes or nodes not modified by this optimization
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
                super::physical_optimizer::split_conjunction_recursive(&predicate, &mut conjuncts);

                let left_child_aliases = self.get_plan_source_table_aliases(&left);
                let right_child_aliases = self.get_plan_source_table_aliases(&right);

                for p_conjunct in conjuncts {
                    let pred_cols = super::physical_optimizer::extract_column_identifiers(&p_conjunct);
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
                    if let Some(left_predicate) = super::physical_optimizer::build_expression_from_conjunctions(left_filters) {
                        new_left = self.push_filter_into_node(new_left, left_predicate);
                    }
                }

                let mut new_right = *right;
                if !right_filters.is_empty() {
                    if let Some(right_predicate) = super::physical_optimizer::build_expression_from_conjunctions(right_filters) {
                        new_right = self.push_filter_into_node(new_right, right_predicate);
                    }
                }
                
                let new_join_plan = PhysicalPlan::NestedLoopJoin {
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                    condition, 
                    join_type,
                };

                if !remaining_predicates.is_empty() {
                    if let Some(final_predicate) = super::physical_optimizer::build_expression_from_conjunctions(remaining_predicates) {
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
                super::physical_optimizer::split_conjunction_recursive(&predicate, &mut conjuncts);

                let left_child_aliases = self.get_plan_source_table_aliases(&left);
                let right_child_aliases = self.get_plan_source_table_aliases(&right);

                for p_conjunct in conjuncts {
                    let pred_cols = super::physical_optimizer::extract_column_identifiers(&p_conjunct);
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
                    if let Some(left_predicate) = super::physical_optimizer::build_expression_from_conjunctions(left_filters) {
                        new_left = self.push_filter_into_node(new_left, left_predicate);
                    }
                }

                let mut new_right = *right;
                if !right_filters.is_empty() {
                    if let Some(right_predicate) = super::physical_optimizer::build_expression_from_conjunctions(right_filters) {
                        new_right = self.push_filter_into_node(new_right, right_predicate);
                    }
                }
                
                let new_join_plan = PhysicalPlan::HashJoin {
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                    condition,
                    join_type,
                };

                if !remaining_predicates.is_empty() {
                    if let Some(final_predicate) = super::physical_optimizer::build_expression_from_conjunctions(remaining_predicates) {
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
} 