// Physical Query Plan Implementation
//
// This module defines the physical plan representation for query execution.

use std::fmt;
use std::sync::{Arc, Mutex};

use crate::query::executor::operators::{Operator, create_table_scan, create_filter, create_projection, 
    create_nested_loop_join, create_hash_join, create_hash_aggregate};
use crate::query::executor::result::QueryResult;
use crate::query::parser::ast::{Expression, JoinType, AggregateFunction, Operator as AstOperator, Value, ColumnDef, DataType};
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
    /// Create Table operator
    CreateTable {
        /// Table name
        table_name: String,
        /// Column definitions
        columns: Vec<ColumnDef>,
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
            PhysicalPlan::CreateTable { table_name, columns } => {
                write!(f, "CreateTable: {} with columns: {}", table_name, columns.iter()
                    .map(|c| format!("{:?}", c))
                    .collect::<Vec<_>>()
                    .join(", "))
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

/// Convert a logical plan to a physical plan
pub fn create_physical_plan(logical_plan: &LogicalPlan) -> PhysicalPlan {
    match logical_plan {
        LogicalPlan::Scan { table_name, alias } => {
            PhysicalPlan::SeqScan {
                table_name: table_name.clone(),
                alias: alias.clone(),
            }
        }
        LogicalPlan::Filter { predicate, input } => {
            PhysicalPlan::Filter {
                input: Box::new(create_physical_plan(input)),
                predicate: predicate.clone(),
            }
        }
        LogicalPlan::Projection { columns, input } => {
            PhysicalPlan::Project {
                input: Box::new(create_physical_plan(input)),
                columns: columns.clone(),
            }
        }
        LogicalPlan::Join { left, right, condition, join_type } => {
            // Choose between hash join and nested loop join based on the condition
            // If it's an equality condition (the most common case), use a hash join
            let is_eq_join = if let Expression::BinaryOp { op, .. } = condition {
                *op == AstOperator::Equals
            } else {
                false
            };
            
            let left_plan = create_physical_plan(left);
            let right_plan = create_physical_plan(right);
            
            if is_eq_join {
                PhysicalPlan::HashJoin {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    condition: condition.clone(),
                    join_type: join_type.clone(),
                }
            } else {
                PhysicalPlan::NestedLoopJoin {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
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

/// Build a complete operator tree from a physical plan
pub fn build_operator_tree(plan: &PhysicalPlan) -> QueryResult<Arc<Mutex<dyn Operator>>> {
    match plan {
        PhysicalPlan::SeqScan { table_name, .. } => {
            create_table_scan(table_name)
        },
        PhysicalPlan::Filter { input, predicate } => {
            // First build the input operator
            let input_op = build_operator_tree(input)?;
            
            // Then create the filter operator
            let predicate_str = expression_to_predicate(predicate);
            
            // Extract table name from the predicate, or use a default
            let table_name = match &**input {
                PhysicalPlan::SeqScan { table_name, .. } => table_name.clone(),
                _ => "default_table".to_string()
            };
            
            create_filter(input_op, predicate.clone(), table_name)
        },
        PhysicalPlan::Project { input, columns } => {
            // First build the input operator
            let input_op = build_operator_tree(input)?;
            
            // Then create the projection operator
            create_projection(input_op, columns.clone())
        },
        PhysicalPlan::NestedLoopJoin { left, right, condition, join_type } => {
            // Build the left and right operators
            let left_op = build_operator_tree(left)?;
            let right_op = build_operator_tree(right)?;
            
            // Convert the join condition to a string
            let condition_str = expression_to_predicate(condition);
            
            // Create the join operator
            let is_left_join = matches!(join_type, JoinType::LeftOuter);
            create_nested_loop_join(left_op, right_op, condition_str, is_left_join)
        },
        PhysicalPlan::HashJoin { left, right, condition, join_type } => {
            // Build the left and right operators
            let left_op = build_operator_tree(left)?;
            let right_op = build_operator_tree(right)?;
            
            // Convert the join condition to a string
            let condition_str = expression_to_predicate(condition);
            
            // Create the hash join operator
            let is_left_join = matches!(join_type, JoinType::LeftOuter);
            create_hash_join(left_op, right_op, condition_str, is_left_join)
        },
        PhysicalPlan::HashAggregate { input, group_by, aggregate_expressions, having } => {
            // Build the input operator
            let input_op = build_operator_tree(input)?;
            
            // Convert the group by expressions to strings
            let group_by_strs: Vec<String> = group_by.iter()
                .map(expression_to_predicate)
                .collect();
            
            // Convert the aggregate expressions to strings
            let agg_strs: Vec<String> = aggregate_expressions.iter()
                .map(expression_to_predicate)
                .collect();
            
            // Convert the having clause to a string (if present)
            let having_str = having.as_ref().map(expression_to_predicate);
            
            // Create the hash aggregate operator
            create_hash_aggregate(input_op, group_by_strs, agg_strs, having_str)
        },
        PhysicalPlan::Materialize { input } => {
            // For now, just return the input operator since materialization is handled
            // in the operator implementations as needed
            build_operator_tree(input)
        },
        PhysicalPlan::CreateTable { table_name, columns } => {
            // For CREATE TABLE, we will handle this at a higher level in the execution engine
            // Here we just create a dummy operator that will be replaced
            let err_msg = format!("Create table operation for {} is handled by the execution engine", table_name);
            Err(crate::query::executor::result::QueryError::ExecutionError(err_msg))
        }
    }
}

/// Add materialization nodes to the physical plan where needed
pub fn add_materialization(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::SeqScan { .. } => plan,
        PhysicalPlan::Filter { input, predicate } => {
            PhysicalPlan::Filter {
                input: Box::new(add_materialization(*input)),
                predicate,
            }
        },
        PhysicalPlan::Project { input, columns } => {
            PhysicalPlan::Project {
                input: Box::new(add_materialization(*input)),
                columns,
            }
        },
        PhysicalPlan::Materialize { .. } => plan,
        PhysicalPlan::NestedLoopJoin { left, right, condition, join_type } => {
            // Nested loop joins benefit from materialization of the right side
            // (which is scanned multiple times)
            let new_right = Box::new(PhysicalPlan::Materialize {
                input: Box::new(add_materialization(*right)),
            });
            
            PhysicalPlan::NestedLoopJoin {
                left: Box::new(add_materialization(*left)),
                right: new_right,
                condition,
                join_type,
            }
        },
        PhysicalPlan::HashJoin { left, right, condition, join_type } => {
            // Hash joins build a hash table from the left input
            // Both inputs benefit from materialization in different ways
            let new_left = Box::new(PhysicalPlan::Materialize {
                input: Box::new(add_materialization(*left)),
            });
            
            PhysicalPlan::HashJoin {
                left: new_left,
                right: Box::new(add_materialization(*right)),
                condition,
                join_type,
            }
        },
        PhysicalPlan::HashAggregate { input, group_by, aggregate_expressions, having } => {
            PhysicalPlan::HashAggregate {
                input: Box::new(add_materialization(*input)),
                group_by,
                aggregate_expressions,
                having,
            }
        },
        PhysicalPlan::CreateTable { table_name, columns } => {
            // CREATE TABLE doesn't need materialization
            PhysicalPlan::CreateTable {
                table_name,
                columns,
            }
        }
    }
}

/// Simple cost model for query optimization
pub struct CostModel;

impl CostModel {
    /// Estimate the cost of a physical plan
    pub fn estimate_cost(plan: &PhysicalPlan) -> f64 {
        match plan {
            PhysicalPlan::SeqScan { .. } => {
                // Sequential scans have a base cost proportional to the table size
                // For now, assume all tables have 1000 rows as a placeholder
                1000.0
            }
            PhysicalPlan::Filter { input, .. } => {
                // Filter cost = input cost * selectivity factor
                // Assume a default selectivity factor of 0.5 (50% of rows pass the filter)
                let input_cost = Self::estimate_cost(input);
                input_cost * 0.5
            }
            PhysicalPlan::Project { input, .. } => {
                // Projection cost = input cost (projections don't reduce cardinality)
                Self::estimate_cost(input)
            }
            PhysicalPlan::Materialize { input } => {
                // Materialization adds a small overhead to the input cost
                let input_cost = Self::estimate_cost(input);
                input_cost * 1.1 // 10% overhead
            }
            PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                // Nested loop join is expensive - O(n*m) where n and m are the sizes of the inputs
                let left_cost = Self::estimate_cost(left);
                let right_cost = Self::estimate_cost(right);
                left_cost * right_cost
            }
            PhysicalPlan::HashJoin { left, right, .. } => {
                // Hash join is O(n+m) where n and m are the sizes of the inputs
                let left_cost = Self::estimate_cost(left);
                let right_cost = Self::estimate_cost(right);
                left_cost + right_cost
            }
            PhysicalPlan::HashAggregate { input, .. } => {
                // Aggregation cost is proportional to the input size
                let input_cost = Self::estimate_cost(input);
                input_cost * 1.5
            }
            PhysicalPlan::CreateTable { .. } => {
                // Create table operations have a fixed cost
                1.0
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
    
    #[test]
    fn test_create_table_physical_plan() {
        // Create a physical plan for a CREATE TABLE operation
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
                nullable: false,
                primary_key: false,
            },
            ColumnDef {
                name: "email".to_string(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
            }
        ];
        
        let create_table_plan = PhysicalPlan::CreateTable {
            table_name: "users".to_string(),
            columns,
        };
        
        // Test converting to string
        let plan_str = format!("{}", create_table_plan);
        assert!(plan_str.contains("CreateTable"));
        assert!(plan_str.contains("users"));
        
        // Test cost model
        let cost = CostModel::estimate_cost(&create_table_plan);
        assert_eq!(cost, 1.0); // Fixed cost for CREATE TABLE
        
        // Test materialization
        let plan_with_mat = add_materialization(create_table_plan.clone());
        assert!(matches!(plan_with_mat, PhysicalPlan::CreateTable { .. }));
        
        // The build_operator_tree should return an error for CREATE TABLE
        // since this is handled at a higher level
        let result = build_operator_tree(&create_table_plan);
        assert!(result.is_err());
    }
} 