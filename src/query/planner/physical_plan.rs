// Physical Query Plan Representation
//
// This module defines the physical plan representation for query execution.

use std::fmt;

use crate::query::parser::ast::{Expression, JoinType, ColumnDef, AlterTableStatement};

/// Represents a specific select expression in the physical plan, 
/// including the expression itself and its final output name (alias or default).
#[derive(Debug, Clone)]
pub struct PhysicalSelectExpression {
    pub expression: Expression,
    pub output_name: String,
}

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
        /// Aggregate select expressions including final output names
        /// These are the aggregates needed for computation (including SELECT and HAVING)
        aggregate_select_expressions: Vec<PhysicalSelectExpression>,
        /// Having clause (optional)
        having: Option<Expression>,
        /// The final list of expressions (group keys or aggregates) in the desired output order,
        /// with their final output names.
        output_select_list: Vec<PhysicalSelectExpression>,
    },
    /// Create Table operator
    CreateTable {
        /// Table name
        table_name: String,
        /// Column definitions
        columns: Vec<ColumnDef>,
    },
    /// Alter Table operator
    AlterTable {
        /// The AlterTable statement from the AST
        statement: AlterTableStatement,
    },
    /// Sort operator (ORDER BY)
    Sort {
        /// Input plan
        input: Box<PhysicalPlan>,
        /// Sort key expressions and direction (true for DESC)
        order_by: Vec<(Expression, bool)>,
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
            PhysicalPlan::HashAggregate { 
                input, 
                group_by, 
                aggregate_select_expressions, 
                having, 
                output_select_list
            } => {
                let agg_str = output_select_list.iter()
                    .map(|e| format!("{} AS {}", e.expression, e.output_name))
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
            PhysicalPlan::AlterTable { statement } => {
                write!(f, "AlterTable: {}", statement)
            }
            PhysicalPlan::Sort { input, order_by } => {
                let order_by_str = order_by
                    .iter()
                    .map(|(expr, is_desc)| {
                        format!("{:?} {}", expr, if *is_desc { "DESC" } else { "ASC" })
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "Sort: [{}]\n  {}", order_by_str, input)
            }
        }
    }
}

/// Convert an expression to a string predicate for operators
pub fn expression_to_predicate(expr: &Expression) -> String {
    use crate::query::parser::ast::{AggregateFunction, Operator as AstOperator, Value};

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
                Value::Boolean(b) => b.to_string(),
            }
        },
        Expression::Function { name, args } => {
            let args_str = args.iter()
                .map(expression_to_predicate)
                .collect::<Vec<_>>()
                .join(", ");
            
            format!("{}({})", name, args_str)
        },
        Expression::Case { operand, when_then_clauses, else_clause } => {
            // Basic string representation for CASE, can be enhanced
            let mut s = "CASE".to_string();
            if let Some(op_expr) = operand {
                s.push_str(&format!(" {}", expression_to_predicate(op_expr)));
            }
            for (when_expr, then_expr) in when_then_clauses {
                s.push_str(&format!(" WHEN {} THEN {}", expression_to_predicate(when_expr), expression_to_predicate(then_expr)));
            }
            if let Some(el_expr) = else_clause {
                s.push_str(&format!(" ELSE {}", expression_to_predicate(el_expr)));
            }
            s.push_str(" END");
            s
        },
        Expression::UnaryOp { op, expr } => {
            let expr_str = expression_to_predicate(expr);
            match op {
                crate::query::parser::ast::UnaryOperator::Minus => format!("-{}", expr_str),
                crate::query::parser::ast::UnaryOperator::Not => format!("NOT {}", expr_str),
            }
        },
        Expression::IsNull { expr, not } => {
            let expr_str = expression_to_predicate(expr);
            if *not {
                format!("({} IS NOT NULL)", expr_str)
            } else {
                format!("({} IS NULL)", expr_str)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::ast::{
        ColumnReference, Operator as AstOperator, Value, AggregateFunction, DataType, ColumnDef
    };

    // Test for each PhysicalPlan variant's display implementation
    #[test]
    fn test_physical_plan_display() {
        // Test SeqScan with alias
        let scan_with_alias = PhysicalPlan::SeqScan {
            table_name: "users".to_string(),
            alias: Some("u".to_string()),
        };
        assert!(format!("{}", scan_with_alias).contains("users as u"));
        
        // Test SeqScan without alias
        let scan_no_alias = PhysicalPlan::SeqScan {
            table_name: "users".to_string(),
            alias: None,
        };
        assert_eq!(format!("{}", scan_no_alias), "SeqScan: users");
        
        // Test Filter
        let filter_plan = PhysicalPlan::Filter {
            input: Box::new(scan_no_alias.clone()),
            predicate: Expression::BinaryOp {
                left: Box::new(Expression::Column(ColumnReference {
                    name: "id".to_string(),
                    table: None,
                })),
                op: AstOperator::GreaterThan,
                right: Box::new(Expression::Literal(Value::Integer(100))),
            },
        };
        let filter_display = format!("{}", filter_plan);
        assert!(filter_display.contains("Filter:"));
        assert!(filter_display.contains("SeqScan: users"));
        
        // Test Project
        let project_plan = PhysicalPlan::Project {
            input: Box::new(scan_no_alias.clone()),
            columns: vec!["id".to_string(), "name".to_string()],
        };
        let project_display = format!("{}", project_plan);
        assert!(project_display.contains("Project: id, name"));
        assert!(project_display.contains("SeqScan: users"));
        
        // Test Materialize
        let materialize_plan = PhysicalPlan::Materialize {
            input: Box::new(scan_no_alias.clone()),
        };
        let materialize_display = format!("{}", materialize_plan);
        assert!(materialize_display.contains("Materialize"));
        assert!(materialize_display.contains("SeqScan: users"));
        
        // Test NestedLoopJoin
        let join_condition = Expression::BinaryOp {
            left: Box::new(Expression::Column(ColumnReference {
                name: "id".to_string(),
                table: Some("users".to_string()),
            })),
            op: AstOperator::Equals,
            right: Box::new(Expression::Column(ColumnReference {
                name: "user_id".to_string(),
                table: Some("orders".to_string()),
            })),
        };
        
        let orders_scan = PhysicalPlan::SeqScan {
            table_name: "orders".to_string(),
            alias: None,
        };
        
        let nested_join_plan = PhysicalPlan::NestedLoopJoin {
            left: Box::new(scan_no_alias.clone()),
            right: Box::new(orders_scan.clone()),
            condition: join_condition.clone(),
            join_type: JoinType::Inner,
        };
        
        let nested_join_display = format!("{}", nested_join_plan);
        assert!(nested_join_display.contains("NestedLoopJoin"));
        assert!(nested_join_display.contains("Inner"));
        assert!(nested_join_display.contains("Left:"));
        assert!(nested_join_display.contains("Right:"));
        
        // Test HashJoin
        let hash_join_plan = PhysicalPlan::HashJoin {
            left: Box::new(scan_no_alias.clone()),
            right: Box::new(orders_scan.clone()),
            condition: join_condition.clone(),
            join_type: JoinType::Inner,
        };
        
        let hash_join_display = format!("{}", hash_join_plan);
        assert!(hash_join_display.contains("HashJoin"));
        assert!(hash_join_display.contains("Inner"));
        assert!(hash_join_display.contains("Left:"));
        assert!(hash_join_display.contains("Right:"));
        
        // Test HashAggregate
        let agg_plan = PhysicalPlan::HashAggregate {
            input: Box::new(PhysicalPlan::SeqScan { // Recreate scan_no_alias locally if needed
                 table_name: "users".to_string(), 
                 alias: None 
            }),
            group_by: vec![Expression::Column(ColumnReference { name: "dept".to_string(), table: None })],
            aggregate_select_expressions: vec![ // Aggregates needed for computation
                PhysicalSelectExpression {
                    expression: Expression::Aggregate {
                        function: AggregateFunction::Count,
                        arg: None,
                    },
                    output_name: "COUNT(*)".to_string(),
                },
            ],
            having: None,
            // Add the field in test case
            output_select_list: vec![ 
                PhysicalSelectExpression { // Group key in output
                    expression: Expression::Column(ColumnReference { name: "dept".to_string(), table: None }),
                    output_name: "dept".to_string(),
                },
                PhysicalSelectExpression { // Aggregate in output
                    expression: Expression::Aggregate {
                        function: AggregateFunction::Count,
                        arg: None,
                    },
                    output_name: "count_all".to_string(), // Example alias
                },
            ]
        };
        let agg_display = format!("{}", agg_plan);
        // Updated assertion based on new Display using output_select_list
        assert!(agg_display.contains("HashAggregate: [dept AS dept, COUNT(*) AS count_all]"));
        assert!(agg_display.contains(" GROUP BY [Column(ColumnReference { table: None, name: \"dept\" })]"));
        assert!(agg_display.contains("SeqScan: users"));
        
        // Test CreateTable
        let column1 = ColumnDef {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: true,
            default_value: None,
        };
        
        let column2 = ColumnDef {
            name: "name".to_string(),
            data_type: DataType::Text,
            nullable: true,
            primary_key: false,
            default_value: None,
        };
        
        let create_plan = PhysicalPlan::CreateTable {
            table_name: "new_table".to_string(),
            columns: vec![column1, column2],
        };
        
        let create_display = format!("{}", create_plan);
        assert!(create_display.contains("CreateTable: new_table"));
        assert!(create_display.contains("id"));
        assert!(create_display.contains("name"));
    }
    
    // Tests for expression_to_predicate function
    #[test]
    fn test_expression_to_predicate() {
        // Test column reference
        let col_expr = Expression::Column(ColumnReference {
            name: "id".to_string(),
            table: None,
        });
        assert_eq!(expression_to_predicate(&col_expr), "id");
        
        // Test column with table reference
        let table_col_expr = Expression::Column(ColumnReference {
            name: "id".to_string(),
            table: Some("users".to_string()),
        });
        assert_eq!(expression_to_predicate(&table_col_expr), "users.id");
        
        // Test literal values
        let int_literal = Expression::Literal(Value::Integer(42));
        assert_eq!(expression_to_predicate(&int_literal), "42");
        
        let float_literal = Expression::Literal(Value::Float(3.14));
        assert_eq!(expression_to_predicate(&float_literal), "3.14");
        
        let string_literal = Expression::Literal(Value::String("test".to_string()));
        assert_eq!(expression_to_predicate(&string_literal), "'test'");
        
        let bool_literal = Expression::Literal(Value::Boolean(true));
        assert_eq!(expression_to_predicate(&bool_literal), "true");
        
        let null_literal = Expression::Literal(Value::Null);
        assert_eq!(expression_to_predicate(&null_literal), "NULL");
        
        // Test binary operations
        let eq_op = Expression::BinaryOp {
            left: Box::new(col_expr.clone()),
            op: AstOperator::Equals,
            right: Box::new(int_literal.clone()),
        };
        assert_eq!(expression_to_predicate(&eq_op), "id = 42");
        
        let and_op = Expression::BinaryOp {
            left: Box::new(eq_op.clone()),
            op: AstOperator::And,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column(ColumnReference {
                    name: "age".to_string(),
                    table: None,
                })),
                op: AstOperator::GreaterThan,
                right: Box::new(Expression::Literal(Value::Integer(18))),
            }),
        };
        assert_eq!(expression_to_predicate(&and_op), "id = 42 AND age > 18");
        
        // Test aggregate functions
        let count_star = Expression::Aggregate {
            function: AggregateFunction::Count,
            arg: None,
        };
        assert_eq!(expression_to_predicate(&count_star), "COUNT(*)");
        
        let count_col = Expression::Aggregate {
            function: AggregateFunction::Count,
            arg: Some(Box::new(col_expr.clone())),
        };
        assert_eq!(expression_to_predicate(&count_col), "COUNT(id)");
        
        let sum_col = Expression::Aggregate {
            function: AggregateFunction::Sum,
            arg: Some(Box::new(col_expr.clone())),
        };
        assert_eq!(expression_to_predicate(&sum_col), "SUM(id)");
        
        // Test custom function
        let func_expr = Expression::Function {
            name: "CONCAT".to_string(),
            args: vec![
                Expression::Column(ColumnReference {
                    name: "first_name".to_string(),
                    table: None,
                }),
                Expression::Literal(Value::String(" ".to_string())),
                Expression::Column(ColumnReference {
                    name: "last_name".to_string(),
                    table: None,
                }),
            ],
        };
        assert_eq!(expression_to_predicate(&func_expr), "CONCAT(first_name, ' ', last_name)");
    }
} 