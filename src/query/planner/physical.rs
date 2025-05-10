// Physical Query Plan Conversion
//
// This module handles conversion from logical plans to physical plans.

use crate::query::planner::logical::LogicalPlan;
use crate::query::planner::physical_plan::PhysicalPlan;
use crate::query::parser::ast::Operator as AstOperator;
use crate::query::parser::ast::Expression;

// Helper to check if the join condition is a simple equality
fn is_equi_join_condition(condition: &Expression) -> bool {
    match condition {
        Expression::BinaryOp { op, .. } => matches!(op, AstOperator::Equals),
        _ => false, // Not a binary operation, or not an equality
    }
}

/// Convert a logical plan to a physical plan
pub fn create_physical_plan(logical_plan: &LogicalPlan) -> PhysicalPlan {
    match logical_plan {
        LogicalPlan::Scan { table_name, alias } => {
            PhysicalPlan::SeqScan {
                table_name: table_name.clone(),
                alias: alias.clone(),
            }
        }
        
        LogicalPlan::Filter { input, predicate } => {
            PhysicalPlan::Filter {
                input: Box::new(create_physical_plan(input)),
                predicate: predicate.clone(),
            }
        }
        
        LogicalPlan::Projection { input, columns } => {
            PhysicalPlan::Project {
                input: Box::new(create_physical_plan(input)),
                columns: columns.clone(),
            }
        }
        
        LogicalPlan::Join { left, right, condition, join_type } => {
            if is_equi_join_condition(condition) {
                PhysicalPlan::HashJoin {
                    left: Box::new(create_physical_plan(left)),
                    right: Box::new(create_physical_plan(right)),
                    condition: condition.clone(),
                    join_type: join_type.clone(),
                }
            } else {
                PhysicalPlan::NestedLoopJoin {
                    left: Box::new(create_physical_plan(left)),
                    right: Box::new(create_physical_plan(right)),
                    condition: condition.clone(),
                    join_type: join_type.clone(),
                }
            }
        }
        
        LogicalPlan::Aggregate { input, group_by, aggregate_expressions, having } => {
            PhysicalPlan::HashAggregate {
                input: Box::new(create_physical_plan(input)),
                group_by: group_by.clone(),
                aggregate_expressions: aggregate_expressions.clone(),
                having: having.clone(),
            }
        }
        
        LogicalPlan::CreateTable { table_name, columns } => {
            PhysicalPlan::CreateTable {
                table_name: table_name.clone(),
                columns: columns.clone(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::ast::{Expression, ColumnReference, Operator, Value, JoinType, AggregateFunction, ColumnDef, DataType};
    
    #[test]
    fn test_simple_physical_plan() {
        // Create a logical scan plan
        let logical_plan = LogicalPlan::Scan {
            table_name: "users".to_string(),
            alias: None,
        };
        
        // Convert to physical plan
        let physical_plan = create_physical_plan(&logical_plan);
        
        // Check the type
        if let PhysicalPlan::SeqScan { table_name, .. } = physical_plan {
            assert_eq!(table_name, "users");
        } else {
            panic!("Expected SeqScan plan");
        }
    }
    
    #[test]
    fn test_filter_physical_plan() {
        // Create a logical filter plan
        let logical_plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table_name: "users".to_string(),
                alias: None,
            }),
            predicate: Expression::BinaryOp {
                left: Box::new(Expression::Column(ColumnReference {
                    name: "id".to_string(),
                    table: None,
                })),
                op: Operator::GreaterThan,
                right: Box::new(Expression::Literal(
                    Value::Integer(100)
                )),
            },
        };
        
        // Convert to physical plan
        let physical_plan = create_physical_plan(&logical_plan);
        
        // Check the type
        if let PhysicalPlan::Filter { input, .. } = physical_plan {
            if let PhysicalPlan::SeqScan { table_name, .. } = *input {
                assert_eq!(table_name, "users");
            } else {
                panic!("Expected SeqScan as input to Filter");
            }
        } else {
            panic!("Expected Filter plan");
        }
    }
    
    #[test]
    fn test_join_physical_plan() {
        // Create a logical join plan
        let logical_plan = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan {
                table_name: "users".to_string(),
                alias: None,
            }),
            right: Box::new(LogicalPlan::Scan {
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
        
        // Convert to physical plan
        let physical_plan = create_physical_plan(&logical_plan);
        
        // Check the type (should be a hash join)
        if let PhysicalPlan::HashJoin { .. } = physical_plan {
            // Successfully created a hash join
        } else {
            panic!("Expected HashJoin plan");
        }
    }

    #[test]
    fn test_aggregate_physical_plan() {
        // Create a logical aggregate plan with GROUP BY and aggregation function
        let logical_plan = LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan {
                table_name: "orders".to_string(),
                alias: None,
            }),
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
        
        // Convert to physical plan
        let physical_plan = create_physical_plan(&logical_plan);
        
        // Check the plan structure
        if let PhysicalPlan::HashAggregate { input, group_by, aggregate_expressions, having } = physical_plan {
            // Check input is a scan
            if let PhysicalPlan::SeqScan { table_name, .. } = *input {
                assert_eq!(table_name, "orders");
            } else {
                panic!("Expected SeqScan as input to HashAggregate");
            }
            
            // Check group by expressions
            assert_eq!(group_by.len(), 1);
            if let Expression::Column(col_ref) = &group_by[0] {
                assert_eq!(col_ref.name, "customer_id");
            } else {
                panic!("Expected Column expression in group_by");
            }
            
            // Check aggregate expressions
            assert_eq!(aggregate_expressions.len(), 1);
            if let Expression::Aggregate { function, arg } = &aggregate_expressions[0] {
                assert!(matches!(function, AggregateFunction::Sum));
                assert!(arg.is_some());
            } else {
                panic!("Expected Aggregate expression");
            }
            
            // Check having
            assert!(having.is_none());
        } else {
            panic!("Expected HashAggregate plan");
        }
    }

    #[test]
    fn test_complex_query_plan() {
        // Create a complex logical plan: SELECT customer_id, SUM(amount) 
        // FROM orders WHERE status = 'completed' 
        // GROUP BY customer_id HAVING SUM(amount) > 1000
        
        // First the filter on status
        let filter_plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table_name: "orders".to_string(),
                alias: None,
            }),
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
        
        // Aggregation expression
        let sum_amount = Expression::Aggregate {
            function: AggregateFunction::Sum,
            arg: Some(Box::new(Expression::Column(ColumnReference {
                name: "amount".to_string(),
                table: None,
            }))),
        };
        
        // The aggregation with group by and having
        let agg_plan = LogicalPlan::Aggregate {
            input: Box::new(filter_plan),
            group_by: vec![
                Expression::Column(ColumnReference {
                    name: "customer_id".to_string(),
                    table: None,
                })
            ],
            aggregate_expressions: vec![sum_amount.clone()],
            having: Some(Expression::BinaryOp {
                left: Box::new(sum_amount),
                op: Operator::GreaterThan,
                right: Box::new(Expression::Literal(Value::Integer(1000))),
            }),
        };
        
        // Finally, the projection
        let logical_plan = LogicalPlan::Projection {
            input: Box::new(agg_plan),
            columns: vec!["customer_id".to_string(), "SUM(amount)".to_string()],
        };
        
        // Convert to physical plan
        let physical_plan = create_physical_plan(&logical_plan);
        
        // Verify the structure recursively
        if let PhysicalPlan::Project { input, columns } = physical_plan {
            // Check projection columns
            assert_eq!(columns, vec!["customer_id", "SUM(amount)"]);
            
            // Check aggregation layer
            if let PhysicalPlan::HashAggregate { input: agg_input, group_by, having, .. } = *input {
                // Verify GROUP BY
                assert_eq!(group_by.len(), 1);
                
                // Verify HAVING clause exists
                assert!(having.is_some());
                
                // Check filter layer
                if let PhysicalPlan::Filter { input: filter_input, predicate } = *agg_input {
                    // Verify predicate is on status
                    if let Expression::BinaryOp { left, .. } = &predicate {
                        if let Expression::Column(col_ref) = &**left {
                            assert_eq!(col_ref.name, "status");
                        } else {
                            panic!("Expected Column in filter predicate");
                        }
                    } else {
                        panic!("Expected BinaryOp in filter predicate");
                    }
                    
                    // Check base scan
                    if let PhysicalPlan::SeqScan { table_name, .. } = *filter_input {
                        assert_eq!(table_name, "orders");
                    } else {
                        panic!("Expected SeqScan as base of query plan");
                    }
                } else {
                    panic!("Expected Filter below HashAggregate");
                }
            } else {
                panic!("Expected HashAggregate below Project");
            }
        } else {
            panic!("Expected Project as root of physical plan");
        }
    }
    
    #[test]
    fn test_create_table_plan() {
        // Define column definitions
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
            },
            ColumnDef {
                name: "email".to_string(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
            },
        ];
        
        // Create a logical CREATE TABLE plan
        let logical_plan = LogicalPlan::CreateTable {
            table_name: "customers".to_string(),
            columns: columns.clone(),
        };
        
        // Convert to physical plan
        let physical_plan = create_physical_plan(&logical_plan);
        
        // Check the plan structure
        if let PhysicalPlan::CreateTable { table_name, columns: phys_columns } = physical_plan {
            assert_eq!(table_name, "customers");
            assert_eq!(columns.len(), phys_columns.len());
            
            // Check each column was converted properly
            for (i, col) in columns.iter().enumerate() {
                assert_eq!(col.name, phys_columns[i].name);
                assert_eq!(col.data_type, phys_columns[i].data_type);
                assert_eq!(col.nullable, phys_columns[i].nullable);
                assert_eq!(col.primary_key, phys_columns[i].primary_key);
            }
        } else {
            panic!("Expected CreateTable plan");
        }
    }
} 