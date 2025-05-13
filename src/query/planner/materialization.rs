// Materialization Optimizer
//
// This module provides materialization optimization logic for physical query plans.

use crate::query::planner::physical_plan::PhysicalPlan;
use std::sync::{Arc, RwLock};
use crate::catalog::Catalog;

/// Materialization Optimizer component
pub struct MaterializationOptimizer {
    catalog: Arc<RwLock<Catalog>>,
}

impl MaterializationOptimizer {
    /// Create a new materialization optimizer
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        MaterializationOptimizer { catalog }
    }
    
    /// Apply materialization optimizations to a physical plan
    pub fn optimize(&self, plan: PhysicalPlan) -> PhysicalPlan {
        match plan {
            PhysicalPlan::NestedLoopJoin { left, right, condition, join_type } => {
                let optimized_left = self.optimize(*left); // Recurse
                let optimized_right = self.optimize(*right); // Recurse
                
                // For nested loop joins, materialize the right side unless it's
                // a simple scan or already materialized
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
                let optimized_left = self.optimize(*left); // Recurse
                let optimized_right = self.optimize(*right); // Recurse
                
                // Hash joins don't need materialization as they build hash tables
                PhysicalPlan::HashJoin {
                    left: Box::new(optimized_left),
                    right: Box::new(optimized_right),
                    condition,
                    join_type,
                }
            }
            // Recursively apply to inputs of other operators
            PhysicalPlan::Filter { input, predicate } => {
                PhysicalPlan::Filter {
                    input: Box::new(self.optimize(*input)),
                    predicate,
                }
            }
            PhysicalPlan::Project { input, columns } => {
                PhysicalPlan::Project {
                    input: Box::new(self.optimize(*input)),
                    columns,
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
            // Base cases: leaf nodes or nodes not modified by this rule
            PhysicalPlan::SeqScan { .. } |
            PhysicalPlan::Materialize { .. } |
            PhysicalPlan::CreateTable { .. } |
            PhysicalPlan::AlterTable { .. } => plan,
        }
    }
} 