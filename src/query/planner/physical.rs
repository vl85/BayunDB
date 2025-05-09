// Physical Query Plan Implementation
//
// This module defines the physical plan representation for query execution.

use std::fmt;
use std::sync::{Arc, Mutex};

use crate::query::executor::operators::{Operator, create_table_scan, create_filter, create_projection, 
    create_nested_loop_join, create_hash_join, create_hash_aggregate};
use crate::query::executor::result::QueryResult;
use crate::query::parser::ast::{Expression, JoinType, AggregateFunction, Operator as AstOperator, Value};
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
    /// Hash Aggregate operator
    HashAggregate {
        /// Input plan
        input: Box<PhysicalPlan>,
        /// Group by expressions
        group_by: Vec<Expression>,
        /// Aggregate expressions (COUNT, SUM, etc.)
        aggregate_expressions: Vec<Expression>,
        /// Having clause (optional)
        having: Option<Expression>,
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
            PhysicalPlan::HashAggregate { input, group_by, aggregate_expressions, having } => {
                let agg_str = aggregate_expressions.iter()
                    .map(|e| format!("{:?}", e))
                    .collect::<Vec<_>>()
                    .join(", ");
                
                let group_by_str = if group_by.is_empty() {
                    "".to_string()
                } else {
                    format!(" GROUP BY [{}]", group_by.iter()
                        .map(|e| format!("{:?}", e))
                        .collect::<Vec<_>>()
                        .join(", "))
                };
                
                let having_str = if let Some(having_expr) = having {
                    format!(" HAVING {:?}", having_expr)
                } else {
                    "".to_string()
                };
                
                write!(f, "HashAggregate: [{}]{}{}\n  {}", agg_str, group_by_str, having_str, input)
            }
        }
    }
}

/// Convert an expression to a string predicate for operators
fn expression_to_predicate(expr: &Expression) -> String {
    match expr {
        Expression::Aggregate { function, arg } => {
            let function_name = match function {
                AggregateFunction::Count => "COUNT",
                AggregateFunction::Sum => "SUM",
                AggregateFunction::Avg => "AVG",
                AggregateFunction::Min => "MIN",
                AggregateFunction::Max => "MAX",
            };
            
            if let Some(arg_expr) = arg {
                match &**arg_expr {
                    Expression::Column(col_ref) => {
                        format!("{}({})", function_name, col_ref.name)
                    },
                    _ => format!("{}(?)", function_name),
                }
            } else {
                format!("{}(*)", function_name)
            }
        },
        Expression::Column(col_ref) => {
            if let Some(table) = &col_ref.table {
                format!("{}.{}", table, col_ref.name)
            } else {
                col_ref.name.clone()
            }
        },
        Expression::BinaryOp { left, op, right } => {
            let left_str = expression_to_predicate(left);
            let right_str = expression_to_predicate(right);
            
            match op {
                AstOperator::Equals => format!("{} = {}", left_str, right_str),
                AstOperator::NotEquals => format!("{} <> {}", left_str, right_str),
                AstOperator::LessThan => format!("{} < {}", left_str, right_str),
                AstOperator::GreaterThan => format!("{} > {}", left_str, right_str),
                AstOperator::LessEquals => format!("{} <= {}", left_str, right_str),
                AstOperator::GreaterEquals => format!("{} >= {}", left_str, right_str),
                AstOperator::And => format!("{} AND {}", left_str, right_str),
                AstOperator::Or => format!("{} OR {}", left_str, right_str),
                AstOperator::Plus => format!("{} + {}", left_str, right_str),
                AstOperator::Minus => format!("{} - {}", left_str, right_str),
                AstOperator::Multiply => format!("{} * {}", left_str, right_str),
                AstOperator::Divide => format!("{} / {}", left_str, right_str),
                AstOperator::Modulo => format!("{} % {}", left_str, right_str),
                AstOperator::Not => format!("NOT {}", right_str),
            }
        },
        Expression::Literal(value) => {
            match value {
                Value::Null => "NULL".to_string(),
                Value::Integer(i) => i.to_string(),
                Value::Float(f) => f.to_string(),
                Value::String(s) => format!("'{}'", s),
                Value::Boolean(b) => if *b { "TRUE".to_string() } else { "FALSE".to_string() },
            }
        },
        Expression::Function { name, args } => {
            let args_str = args.iter()
                .map(|arg| expression_to_predicate(arg))
                .collect::<Vec<_>>()
                .join(", ");
            
            format!("{}({})", name, args_str)
        },
    }
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
        LogicalPlan::Aggregate { input, group_by, aggregate_expressions, having } => {
            // Convert the input to a physical plan
            let physical_input = create_physical_plan(input);
            
            // Choose the best aggregation strategy based on the query characteristics
            // For now, we always use HashAggregate as it's generally more efficient
            // In a real system, we might choose sort-based aggregation for certain scenarios
            
            // Criteria that might favor sort-based aggregation:
            // 1. Input is already sorted on the group-by keys
            // 2. Very large number of groups that might not fit in memory
            // 3. Result needs to be sorted by group-by keys
            
            // For this implementation, we'll use hash-based aggregation for all cases
            // but in a real system this would be a cost-based decision
            PhysicalPlan::HashAggregate {
                input: Box::new(physical_input),
                group_by: group_by.clone(),
                aggregate_expressions: aggregate_expressions.clone(),
                having: having.clone(),
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
            let input_op = build_operator_tree(input)?;
            
            // Convert predicate expression to a string for the filter operator
            // This is a simple implementation for now
            let predicate_str = expression_to_predicate(predicate);
            
            // In a real implementation, we'd use the actual predicate expression
            // to build a more sophisticated filter operator
            let predicate_expr = predicate.clone();
            
            // Create the filter operator with the input and predicate
            create_filter(input_op, predicate_expr, "default_table".to_string())
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
            let is_left_join = matches!(join_type, JoinType::LeftOuter);
            
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
            let is_left_join = matches!(join_type, JoinType::LeftOuter);
            
            // Create the hash join operator
            create_hash_join(left_op, right_op, condition_str, is_left_join)
        }
        PhysicalPlan::HashAggregate { input, group_by, aggregate_expressions, having } => {
            // Build the input operator
            let input_op = build_operator_tree(input)?;
            
            // Convert expressions to strings for now
            // This is a simplified approach - in a real system, we'd have a more 
            // sophisticated mechanism to evaluate expressions
            
            // Convert group_by expressions to strings - for modulo expressions, use aliases
            let group_by_columns = group_by.iter()
                .map(|expr| {
                    match expr {
                        // For binary op expressions like (id % 5), use a computed column name
                        Expression::BinaryOp { left, op, right } if *op == AstOperator::Modulo => {
                            if let (Expression::Column(col_ref), Expression::Literal(Value::Integer(val))) 
                                = (&**left, &**right) {
                                format!("{}_mod_{}", col_ref.name, val)
                            } else {
                                format!("{}", expression_to_predicate(expr))
                            }
                        },
                        // For normal column references, use the column name
                        Expression::Column(col_ref) => {
                            col_ref.name.clone()
                        },
                        // For other expressions, convert to string
                        _ => format!("{}", expression_to_predicate(expr))
                    }
                })
                .collect();
                
            // Convert aggregate expressions to strings
            let agg_expr_strings = aggregate_expressions.iter()
                .map(|expr| format!("{}", expression_to_predicate(expr)))
                .collect();
                
            // Convert having clause if present
            let having_str = having.as_ref()
                .map(|expr| format!("{}", expression_to_predicate(expr)));
                
            // Create the hash aggregate operator
            create_hash_aggregate(input_op, group_by_columns, agg_expr_strings, having_str)
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
        PhysicalPlan::HashAggregate { input, group_by, aggregate_expressions, having } => {
            // Add materialization to the input
            PhysicalPlan::HashAggregate {
                input: Box::new(add_materialization(*input)),
                group_by: group_by.clone(),
                aggregate_expressions: aggregate_expressions.clone(),
                having: having.clone(),
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
            PhysicalPlan::HashAggregate { input, group_by, aggregate_expressions, having } => {
                // Base cost for hash table creation and management
                let base_cost = 50.0;
                
                // Cost increases with number of group-by expressions (more complex key computation)
                let group_by_cost = 10.0 * group_by.len() as f64;
                
                // Cost increases with number of aggregate expressions to compute
                let agg_expr_cost = 5.0 * aggregate_expressions.len() as f64;
                
                // Additional cost if we have a HAVING clause (post-filtering)
                let having_cost = if having.is_some() { 20.0 } else { 0.0 };
                
                // Total cost is base + group_by + agg_expr + having + input
                base_cost + group_by_cost + agg_expr_cost + having_cost + Self::estimate_cost(input)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::ast::{ColumnReference, Value, Operator as AstOperator, AggregateFunction, TableReference, SelectStatement, SelectColumn};
    use crate::query::planner::logical::build_logical_plan;
    
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
    
    #[test]
    fn test_aggregate_physical_plan() {
        // Create a SELECT statement with GROUP BY and aggregate functions
        let stmt = SelectStatement {
            columns: vec![
                SelectColumn::Column(ColumnReference {
                    table: None,
                    name: "department_id".to_string(),
                }),
                SelectColumn::Expression { 
                    expr: Box::new(Expression::Aggregate { 
                        function: AggregateFunction::Count, 
                        arg: None,
                    }),
                    alias: Some("count".to_string()),
                },
                SelectColumn::Expression { 
                    expr: Box::new(Expression::Aggregate { 
                        function: AggregateFunction::Sum, 
                        arg: Some(Box::new(Expression::Column(ColumnReference {
                            table: None,
                            name: "salary".to_string(),
                        }))),
                    }),
                    alias: Some("total_salary".to_string()),
                },
            ],
            from: vec![TableReference {
                name: "employees".to_string(),
                alias: None,
            }],
            where_clause: Some(Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column(ColumnReference {
                    table: None,
                    name: "status".to_string(),
                })),
                op: AstOperator::Equals,
                right: Box::new(Expression::Literal(Value::String("active".to_string()))),
            })),
            joins: vec![],
            group_by: Some(vec![
                Expression::Column(ColumnReference {
                    table: None,
                    name: "department_id".to_string(),
                }),
            ]),
            having: Some(Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Aggregate { 
                    function: AggregateFunction::Count, 
                    arg: None,
                }),
                op: AstOperator::GreaterThan,
                right: Box::new(Expression::Literal(Value::Integer(5))),
            })),
        };
        
        // Build logical plan
        let logical_plan = build_logical_plan(&stmt);
        
        // Convert to physical plan
        let physical_plan = create_physical_plan(&logical_plan);
        
        // Verify the structure matches our expectations
        if let PhysicalPlan::Project { input, columns } = &physical_plan {
            assert_eq!(columns.len(), 3); // department_id, count, total_salary
            
            if let PhysicalPlan::HashAggregate { input, group_by, aggregate_expressions, having } = &**input {
                // Verify group by
                assert_eq!(group_by.len(), 1);
                
                // Verify aggregate expressions
                assert_eq!(aggregate_expressions.len(), 2); // COUNT, SUM
                
                // Verify having clause
                assert!(having.is_some());
                
                // Verify input is a Filter
                if let PhysicalPlan::Filter { input, .. } = &**input {
                    // Verify filter input is a SeqScan
                    if let PhysicalPlan::SeqScan { table_name, .. } = &**input {
                        assert_eq!(table_name, "employees");
                    } else {
                        panic!("Expected SeqScan operation under Filter");
                    }
                } else {
                    panic!("Expected Filter operation under HashAggregate");
                }
            } else {
                panic!("Expected HashAggregate operation under Project");
            }
        } else {
            panic!("Expected Project as root operation");
        }
    }
} 