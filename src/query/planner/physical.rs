// Physical Query Plan Implementation
//
// This module defines the physical plan representation for query execution.

use std::fmt;
use std::sync::{Arc, Mutex};

use crate::query::executor::operators::{Operator, create_table_scan, create_filter, create_projection, 
    create_nested_loop_join, create_hash_join};
use crate::query::executor::result::QueryResult;
use crate::query::parser::ast::{Expression, JoinType};
use crate::query::planner::logical::LogicalPlan;

/// Represents a node in the physical query plan
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    /// Table scan operator (sequential scan)
    SeqScan {
        /// Table name
        table_name: String,
        /// Table alias (if any)
        alias: Option<String>,
    },
    /// Filter operator (selection)
    Filter {
        /// Input plan
        input: Box<PhysicalPlan>,
        /// Predicate expression
        predicate: Expression,
    },
    /// Projection operator
    Project {
        /// Input plan
        input: Box<PhysicalPlan>,
        /// Output columns
        columns: Vec<String>,
    },
    /// Material operator (materialize intermediate results)
    Materialize {
        /// Input plan
        input: Box<PhysicalPlan>,
    },
    /// Nested Loop Join operator
    NestedLoopJoin {
        /// Left input plan
        left: Box<PhysicalPlan>,
        /// Right input plan
        right: Box<PhysicalPlan>,
        /// Join condition
        condition: Expression,
        /// Join type
        join_type: JoinType,
    },
    /// Hash Join operator
    HashJoin {
        /// Left input plan (build side)
        left: Box<PhysicalPlan>,
        /// Right input plan (probe side)
        right: Box<PhysicalPlan>,
        /// Join condition
        condition: Expression,
        /// Join type
        join_type: JoinType,
    },
}

impl fmt::Display for PhysicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PhysicalPlan::SeqScan { table_name, alias } => {
                if let Some(a) = alias {
                    write!(f, "SeqScan: {} as {}", table_name, a)
                } else {
                    write!(f, "SeqScan: {}", table_name)
                }
            }
            PhysicalPlan::Filter { input, predicate } => {
                write!(f, "Filter: {:?}\n  {}", predicate, input)
            }
            PhysicalPlan::Project { input, columns } => {
                write!(f, "Project: {}\n  {}", columns.join(", "), input)
            }
            PhysicalPlan::Materialize { input } => {
                write!(f, "Materialize\n  {}", input)
            }
            PhysicalPlan::NestedLoopJoin { left, right, condition, join_type } => {
                write!(f, "NestedLoopJoin ({:?}): {:?}\n  Left: {}\n  Right: {}", 
                       join_type, condition, left, right)
            }
            PhysicalPlan::HashJoin { left, right, condition, join_type } => {
                write!(f, "HashJoin ({:?}): {:?}\n  Left: {}\n  Right: {}", 
                       join_type, condition, left, right)
            }
        }
    }
}

/// Convert an expression to a string predicate
/// This is a simplified version for now - in a real system you'd have
/// a predicate evaluator that can handle complex expressions
fn expression_to_predicate(expr: &Expression) -> String {
    format!("{:?}", expr) // Basic transformation for now
}

/// Create a physical plan from a logical plan
pub fn create_physical_plan(logical_plan: &LogicalPlan) -> PhysicalPlan {
    match logical_plan {
        LogicalPlan::Scan { table_name, alias } => {
            PhysicalPlan::SeqScan {
                table_name: table_name.clone(),
                alias: alias.clone(),
            }
        }
        LogicalPlan::Filter { predicate, input } => {
            // First convert the input to a physical plan
            let physical_input = create_physical_plan(input);
            
            // Then create the filter operator
            PhysicalPlan::Filter {
                input: Box::new(physical_input),
                predicate: predicate.clone(),
            }
        }
        LogicalPlan::Projection { columns, input } => {
            // First convert the input to a physical plan
            let physical_input = create_physical_plan(input);
            
            // Then create the projection operator
            PhysicalPlan::Project {
                input: Box::new(physical_input),
                columns: columns.clone(),
            }
        }
        LogicalPlan::Join { left, right, condition, join_type } => {
            // Convert both inputs to physical plans
            let physical_left = create_physical_plan(left);
            let physical_right = create_physical_plan(right);
            
            // Choose between hash join and nested loop join based on join condition
            // For now, use hash join for equi-joins and nested loop for others
            // In a real system, this would be based on cost estimation
            match condition {
                Expression::BinaryOp { op, .. } if op.is_equality() => {
                    // Use hash join for equality conditions (more efficient)
                    PhysicalPlan::HashJoin {
                        left: Box::new(physical_left),
                        right: Box::new(physical_right),
                        condition: condition.clone(),
                        join_type: join_type.clone(),
                    }
                }
                _ => {
                    // Use nested loop join for other conditions
                    PhysicalPlan::NestedLoopJoin {
                        left: Box::new(physical_left),
                        right: Box::new(physical_right),
                        condition: condition.clone(),
                        join_type: join_type.clone(),
                    }
                }
            }
        }
    }
}

/// Build an executable operator tree from a physical plan
pub fn build_operator_tree(plan: &PhysicalPlan) -> QueryResult<Arc<Mutex<dyn Operator>>> {
    match plan {
        PhysicalPlan::SeqScan { table_name, .. } => {
            // Create a table scan operator
            create_table_scan(table_name)
        }
        PhysicalPlan::Filter { input, predicate } => {
            // First build the input operator
            let input_op = build_operator_tree(input)?;
            
            // Then create the filter operator with the input
            // We convert the expression to a string predicate for simplicity
            let predicate_str = expression_to_predicate(predicate);
            create_filter(input_op, predicate_str)
        }
        PhysicalPlan::Project { input, columns } => {
            // First build the input operator
            let input_op = build_operator_tree(input)?;
            
            // Then create the projection operator with the input
            create_projection(input_op, columns.clone())
        }
        PhysicalPlan::Materialize { input } => {
            // For now, we'll just pass through to the input operator
            // In a real system, you'd create a materializing operator
            build_operator_tree(input)
        }
        PhysicalPlan::NestedLoopJoin { left, right, condition, join_type } => {
            // Build the left and right input operators
            let left_op = build_operator_tree(left)?;
            let right_op = build_operator_tree(right)?;
            
            // Convert the condition to a string
            let condition_str = expression_to_predicate(condition);
            
            // Determine if this is a left join
            let is_left_join = *join_type == JoinType::LeftOuter;
            
            // Create the nested loop join operator
            create_nested_loop_join(left_op, right_op, condition_str, is_left_join)
        }
        PhysicalPlan::HashJoin { left, right, condition, join_type } => {
            // Build the left and right input operators
            let left_op = build_operator_tree(left)?;
            let right_op = build_operator_tree(right)?;
            
            // Convert the condition to a string
            let condition_str = expression_to_predicate(condition);
            
            // Determine if this is a left join
            let is_left_join = *join_type == JoinType::LeftOuter;
            
            // Create the hash join operator
            create_hash_join(left_op, right_op, condition_str, is_left_join)
        }
    }
}

/// Add materialization hints to a physical plan where needed
pub fn add_materialization(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Project { input, columns } => {
            // Add materialization if the input is not already materialized
            // and is a complex operation that could benefit from materialization
            let materialized_input = match *input {
                PhysicalPlan::Filter { .. } => {
                    // Filters with complex predicates might benefit from materialization
                    Box::new(PhysicalPlan::Materialize {
                        input: Box::new(add_materialization(*input)),
                    })
                }
                _ => {
                    // Otherwise just recursively process the input
                    Box::new(add_materialization(*input))
                }
            };
            
            PhysicalPlan::Project {
                input: materialized_input,
                columns,
            }
        }
        PhysicalPlan::Filter { input, predicate } => {
            // Recursively process the input
            PhysicalPlan::Filter {
                input: Box::new(add_materialization(*input)),
                predicate,
            }
        }
        PhysicalPlan::NestedLoopJoin { left, right, condition, join_type } => {
            // Add materialization to both sides of the join
            PhysicalPlan::NestedLoopJoin {
                left: Box::new(add_materialization(*left)),
                right: Box::new(add_materialization(*right)),
                condition,
                join_type,
            }
        }
        PhysicalPlan::HashJoin { left, right, condition, join_type } => {
            // Add materialization to both sides of the join
            PhysicalPlan::HashJoin {
                left: Box::new(add_materialization(*left)),
                right: Box::new(add_materialization(*right)),
                condition,
                join_type,
            }
        }
        // Base case - no materialization needed
        _ => plan,
    }
}

/// Cost model for physical plans
/// This is a simplified implementation - in a real system this would be more complex
pub struct CostModel;

impl CostModel {
    /// Calculate the estimated cost of a physical plan
    pub fn estimate_cost(plan: &PhysicalPlan) -> f64 {
        match plan {
            PhysicalPlan::SeqScan { .. } => {
                // Assume sequential scan has a base cost of 100
                100.0
            }
            PhysicalPlan::Filter { input, .. } => {
                // Filters add some overhead plus the cost of the input
                10.0 + Self::estimate_cost(input)
            }
            PhysicalPlan::Project { input, .. } => {
                // Projections add minimal overhead
                5.0 + Self::estimate_cost(input)
            }
            PhysicalPlan::Materialize { input } => {
                // Materialization is expensive but can save in repeated access
                50.0 + Self::estimate_cost(input)
            }
            PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                // Nested loop joins are very expensive - cost is product of inputs
                Self::estimate_cost(left) * Self::estimate_cost(right)
            }
            PhysicalPlan::HashJoin { left, right, .. } => {
                // Hash joins are cheaper than nested loop - cost is sum plus overhead
                75.0 + Self::estimate_cost(left) + Self::estimate_cost(right)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::ast::{ColumnReference, Value, Operator as AstOperator};
    
    #[test]
    fn test_simple_physical_plan() {
        // Create a simple logical plan
        let logical_plan = LogicalPlan::Scan {
            table_name: "users".to_string(),
            alias: None,
        };
        
        // Convert to physical plan
        let physical_plan = create_physical_plan(&logical_plan);
        
        // Verify the structure
        match physical_plan {
            PhysicalPlan::SeqScan { table_name, alias } => {
                assert_eq!(table_name, "users");
                assert!(alias.is_none());
            }
            _ => panic!("Expected SeqScan as the physical plan"),
        }
    }
    
    #[test]
    fn test_filter_physical_plan() {
        // Create a logical plan with a filter
        let predicate = Expression::BinaryOp {
            left: Box::new(Expression::Column(ColumnReference {
                table: None,
                name: "age".to_string(),
            })),
            op: AstOperator::GreaterThan,
            right: Box::new(Expression::Literal(Value::Integer(30))),
        };
        
        let logical_plan = LogicalPlan::Filter {
            predicate: predicate.clone(),
            input: Box::new(LogicalPlan::Scan {
                table_name: "users".to_string(),
                alias: None,
            }),
        };
        
        // Convert to physical plan
        let physical_plan = create_physical_plan(&logical_plan);
        
        // Verify the structure
        match physical_plan {
            PhysicalPlan::Filter { input, predicate: expr } => {
                assert_eq!(format!("{:?}", expr), format!("{:?}", predicate));
                
                match *input {
                    PhysicalPlan::SeqScan { table_name, .. } => {
                        assert_eq!(table_name, "users");
                    }
                    _ => panic!("Expected SeqScan under Filter"),
                }
            }
            _ => panic!("Expected Filter as the physical plan"),
        }
    }
    
    #[test]
    fn test_projection_physical_plan() {
        // Create a logical plan with a projection
        let columns = vec!["id".to_string(), "name".to_string()];
        
        let logical_plan = LogicalPlan::Projection {
            columns: columns.clone(),
            input: Box::new(LogicalPlan::Scan {
                table_name: "users".to_string(),
                alias: None,
            }),
        };
        
        // Convert to physical plan
        let physical_plan = create_physical_plan(&logical_plan);
        
        // Verify the structure
        match physical_plan {
            PhysicalPlan::Project { input, columns: cols } => {
                assert_eq!(cols, columns);
                
                match *input {
                    PhysicalPlan::SeqScan { table_name, .. } => {
                        assert_eq!(table_name, "users");
                    }
                    _ => panic!("Expected SeqScan under Project"),
                }
            }
            _ => panic!("Expected Project as the physical plan"),
        }
    }
} 