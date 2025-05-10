// Cost Model for Query Optimization
//
// This module provides cost estimation for query plans to aid optimization decisions.

use crate::query::planner::physical_plan::PhysicalPlan;

/// A simple cost model for physical query plans
pub struct CostModel;

impl CostModel {
    /// Create a new cost model
    pub fn new() -> Self {
        CostModel
    }

    /// Estimate the cost of executing a physical plan
    pub fn estimate_cost(&self, plan: &PhysicalPlan) -> f64 {
        match plan {
            PhysicalPlan::SeqScan { .. } => {
                // Simple scan cost, in a real system would be based on table statistics
                100.0
            }
            
            PhysicalPlan::Filter { input, .. } => {
                // Cost of input + filter operation (assume 50% selectivity)
                let input_cost = self.estimate_cost(input);
                input_cost + (input_cost * 0.1)  // 10% overhead for filter operation
            }
            
            PhysicalPlan::Project { input, .. } => {
                // Cost of input + small overhead for projection
                let input_cost = self.estimate_cost(input);
                input_cost + (input_cost * 0.05)  // 5% overhead for projection
            }
            
            PhysicalPlan::Materialize { input } => {
                // Cost of input + cost to materialize
                let input_cost = self.estimate_cost(input);
                input_cost * 2.0  // Assume materialization doubles the cost
            }
            
            PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                // Nested loop is expensive - product of costs
                let left_cost = self.estimate_cost(left);
                let right_cost = self.estimate_cost(right);
                
                // The right side is scanned for each row of the left
                left_cost + (left_cost * right_cost)
            }
            
            PhysicalPlan::HashJoin { left, right, .. } => {
                // Hash join is generally more efficient than nested loop
                let left_cost = self.estimate_cost(left);
                let right_cost = self.estimate_cost(right);
                
                // Cost to build hash table + probe
                left_cost + right_cost + (left_cost.max(right_cost) * 0.2)
            }
            
            PhysicalPlan::HashAggregate { input, .. } => {
                // Cost of input + aggregation overhead
                let input_cost = self.estimate_cost(input);
                input_cost + (input_cost * 0.3)  // 30% overhead for aggregation
            }
            
            PhysicalPlan::CreateTable { .. } => {
                // DDL operations typically have a fixed cost
                10.0
            }
        }
    }
    
    /// Determine if a hash join would be more efficient than a nested loop join
    pub fn should_use_hash_join(&self, left_size: f64, right_size: f64) -> bool {
        // Approximate heuristic: use hash join if the product of the sizes is large
        left_size * right_size > 1000.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::ast::{Expression, ColumnReference, Operator, Value, JoinType, AggregateFunction};
    
    #[test]
    fn test_estimate_scan_cost() {
        let plan = PhysicalPlan::SeqScan {
            table_name: "users".to_string(),
            alias: None,
        };
        
        let cost_model = CostModel::new();
        let cost = cost_model.estimate_cost(&plan);
        
        assert!(cost > 0.0);
    }
    
    #[test]
    fn test_estimate_join_costs() {
        // Create a nested loop join and a hash join with the same inputs
        let left = Box::new(PhysicalPlan::SeqScan {
            table_name: "users".to_string(), 
            alias: None,
        });
        
        let right = Box::new(PhysicalPlan::SeqScan {
            table_name: "orders".to_string(),
            alias: None,
        });
        
        let condition = Expression::BinaryOp {
            left: Box::new(Expression::Column(ColumnReference {
                name: "id".to_string(),
                table: Some("users".to_string()),
            })),
            op: Operator::Equals,
            right: Box::new(Expression::Column(ColumnReference {
                name: "user_id".to_string(),
                table: Some("orders".to_string()),
            })),
        };
        
        let nested_loop_plan = PhysicalPlan::NestedLoopJoin {
            left: left.clone(),
            right: right.clone(),
            condition: condition.clone(),
            join_type: JoinType::Inner,
        };
        
        let hash_join_plan = PhysicalPlan::HashJoin {
            left,
            right,
            condition,
            join_type: JoinType::Inner,
        };
        
        let cost_model = CostModel::new();
        let nested_loop_cost = cost_model.estimate_cost(&nested_loop_plan);
        let hash_join_cost = cost_model.estimate_cost(&hash_join_plan);
        
        // Hash join should generally be cheaper than nested loop
        assert!(hash_join_cost < nested_loop_cost);
    }
    
    #[test]
    fn test_complex_plan_cost() {
        // Build a complex query plan
        // SELECT customer_id, SUM(amount) FROM orders WHERE status = 'completed' GROUP BY customer_id
        
        // Base scan
        let scan = PhysicalPlan::SeqScan {
            table_name: "orders".to_string(),
            alias: None,
        };
        
        // Filter
        let filter = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: Expression::BinaryOp {
                left: Box::new(Expression::Column(ColumnReference {
                    name: "status".to_string(),
                    table: None,
                })),
                op: Operator::Equals,
                right: Box::new(Expression::Literal(
                    Value::String("completed".to_string())
                )),
            },
        };
        
        // Aggregation
        let agg = PhysicalPlan::HashAggregate {
            input: Box::new(filter),
            group_by: vec![
                Expression::Column(ColumnReference {
                    name: "customer_id".to_string(),
                    table: None,
                })
            ],
            aggregate_expressions: vec![
                Expression::Aggregate {
                    function: AggregateFunction::Sum,
                    arg: Some(Box::new(Expression::Column(ColumnReference {
                        name: "amount".to_string(),
                        table: None,
                    }))),
                }
            ],
            having: None,
        };
        
        // Calculate the cost
        let cost_model = CostModel::new();
        let cost = cost_model.estimate_cost(&agg);
        
        // Verify the cost is calculated correctly
        let expected_scan_cost = 100.0;
        let expected_filter_cost = expected_scan_cost + (expected_scan_cost * 0.1);
        let expected_agg_cost = expected_filter_cost + (expected_filter_cost * 0.3);
        
        assert_eq!(cost, expected_agg_cost);
    }
    
    #[test]
    fn test_materialization_cost() {
        // Test that materialization increases cost
        let scan = PhysicalPlan::SeqScan {
            table_name: "users".to_string(),
            alias: None,
        };
        
        let materialized = PhysicalPlan::Materialize {
            input: Box::new(scan.clone()),
        };
        
        let cost_model = CostModel::new();
        let scan_cost = cost_model.estimate_cost(&scan);
        let materialized_cost = cost_model.estimate_cost(&materialized);
        
        // Materialization should double the cost in our model
        assert_eq!(materialized_cost, scan_cost * 2.0);
    }
    
    #[test]
    fn test_cost_with_joins() {
        // Create a complex plan with multiple joins
        
        // Base tables
        let users = PhysicalPlan::SeqScan {
            table_name: "users".to_string(),
            alias: None,
        };
        
        let orders = PhysicalPlan::SeqScan {
            table_name: "orders".to_string(),
            alias: None,
        };
        
        let products = PhysicalPlan::SeqScan {
            table_name: "products".to_string(),
            alias: None,
        };
        
        // First join: users and orders
        let join1 = PhysicalPlan::HashJoin {
            left: Box::new(users.clone()),
            right: Box::new(orders.clone()),
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
        
        // Second join: result of first join with products
        let join2 = PhysicalPlan::HashJoin {
            left: Box::new(join1),
            right: Box::new(products.clone()),
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
        
        // Calculate cost
        let cost_model = CostModel::new();
        let cost = cost_model.estimate_cost(&join2);
        
        // Verify the cost is higher than any individual component
        let users_cost = cost_model.estimate_cost(&users);
        let orders_cost = cost_model.estimate_cost(&orders);
        let products_cost = cost_model.estimate_cost(&products);
        
        assert!(cost > users_cost);
        assert!(cost > orders_cost);
        assert!(cost > products_cost);
        assert!(cost > users_cost + orders_cost + products_cost);
    }
    
    #[test]
    fn test_should_use_hash_join() {
        let cost_model = CostModel::new();
        
        // Small tables: nested loop is fine
        assert!(!cost_model.should_use_hash_join(10.0, 10.0));
        
        // Medium tables: one big, one small
        assert!(!cost_model.should_use_hash_join(1000.0, 1.0));
        
        // Large tables: use hash join
        assert!(cost_model.should_use_hash_join(100.0, 20.0));
        assert!(cost_model.should_use_hash_join(1000.0, 2.0));
    }
    
    #[test]
    fn test_nested_plan_cost() {
        // Create a deeply nested plan to test recursive cost estimation
        
        // Base scan
        let scan = PhysicalPlan::SeqScan {
            table_name: "data".to_string(),
            alias: None,
        };
        
        // First filter
        let filter1 = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: Expression::BinaryOp {
                left: Box::new(Expression::Column(ColumnReference {
                    name: "value".to_string(),
                    table: None,
                })),
                op: Operator::GreaterThan,
                right: Box::new(Expression::Literal(Value::Integer(100))),
            },
        };
        
        // Second filter
        let filter2 = PhysicalPlan::Filter {
            input: Box::new(filter1),
            predicate: Expression::BinaryOp {
                left: Box::new(Expression::Column(ColumnReference {
                    name: "category".to_string(),
                    table: None,
                })),
                op: Operator::Equals,
                right: Box::new(Expression::Literal(Value::String("A".to_string()))),
            },
        };
        
        // Projection
        let project = PhysicalPlan::Project {
            input: Box::new(filter2),
            columns: vec!["id".to_string(), "value".to_string()],
        };
        
        // Cost model
        let cost_model = CostModel::new();
        let cost = cost_model.estimate_cost(&project);
        
        // Calculate expected cost manually
        let base_cost = 100.0; // Scan
        let filter1_cost = base_cost + (base_cost * 0.1); // First filter
        let filter2_cost = filter1_cost + (filter1_cost * 0.1); // Second filter
        let expected_cost = filter2_cost + (filter2_cost * 0.05); // Projection
        
        assert_eq!(cost, expected_cost);
    }
} 