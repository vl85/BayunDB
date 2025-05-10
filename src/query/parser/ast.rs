// SQL Abstract Syntax Tree (AST) Implementation
//
// This module defines the AST nodes for representing parsed SQL queries.

use std::fmt;

/// Represents a SQL statement
#[derive(Debug, Clone)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Create(CreateStatement),
}

/// SELECT statement representation
#[derive(Debug, Clone)]
pub struct SelectStatement {
    /// Columns in SELECT clause
    pub columns: Vec<SelectColumn>,
    /// FROM clause table references
    pub from: Vec<TableReference>,
    /// WHERE clause (optional)
    pub where_clause: Option<Box<Expression>>,
    /// JOIN clauses (optional)
    pub joins: Vec<JoinClause>,
    /// GROUP BY clause (optional)
    pub group_by: Option<Vec<Expression>>,
    /// HAVING clause (optional)
    pub having: Option<Box<Expression>>,
}

/// Column in a SELECT statement
#[derive(Debug, Clone)]
pub enum SelectColumn {
    /// All columns (*)
    Wildcard,
    /// Regular column reference
    Column(ColumnReference),
    /// Expression with optional alias
    Expression {
        expr: Box<Expression>,
        alias: Option<String>,
    },
}

/// Column reference (could be qualified with table name)
#[derive(Debug, Clone)]
pub struct ColumnReference {
    pub table: Option<String>,
    pub name: String,
}

/// Table reference in FROM clause
#[derive(Debug, Clone)]
pub struct TableReference {
    pub name: String,
    pub alias: Option<String>,
}

/// JOIN clause representation
#[derive(Debug, Clone)]
pub struct JoinClause {
    /// The type of join
    pub join_type: JoinType,
    /// The table being joined
    pub table: TableReference,
    /// The join condition (ON clause)
    pub condition: Box<Expression>,
}

/// Types of SQL JOINs
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    /// INNER JOIN (default)
    Inner,
    /// LEFT OUTER JOIN
    LeftOuter,
    /// RIGHT OUTER JOIN
    RightOuter,
    /// FULL OUTER JOIN
    FullOuter,
    /// CROSS JOIN (cartesian product)
    Cross,
}

/// Aggregate function types
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Expression in SQL
#[derive(Debug, Clone)]
pub enum Expression {
    /// Literal value
    Literal(Value),
    /// Column reference
    Column(ColumnReference),
    /// Binary operation (e.g., a + b, x = y)
    BinaryOp {
        left: Box<Expression>,
        op: Operator,
        right: Box<Expression>,
    },
    /// Function call
    Function {
        name: String,
        args: Vec<Expression>,
    },
    /// Aggregate function
    Aggregate {
        function: AggregateFunction,
        arg: Option<Box<Expression>>,
    },
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expression::Literal(val) => write!(f, "{}", val),
            Expression::Column(col_ref) => {
                if let Some(table) = &col_ref.table {
                    write!(f, "{}.{}", table, col_ref.name)
                } else {
                    write!(f, "{}", col_ref.name)
                }
            }
            Expression::BinaryOp { left, op, right } => write!(f, "({} {} {})", left, op, right),
            Expression::Function { name, args } => {
                let arg_list = args.iter().map(|arg| arg.to_string()).collect::<Vec<String>>().join(", ");
                write!(f, "{}({})", name, arg_list)
            }
            Expression::Aggregate { function, arg } => {
                let func_name = match function {
                    AggregateFunction::Count => "COUNT",
                    AggregateFunction::Sum => "SUM",
                    AggregateFunction::Avg => "AVG",
                    AggregateFunction::Min => "MIN",
                    AggregateFunction::Max => "MAX",
                };
                if let Some(arg_expr) = arg {
                    write!(f, "{}({})", func_name, arg_expr)
                } else {
                    write!(f, "{}(*)", func_name)
                }
            }
        }
    }
}

/// SQL values
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::String(s) => write!(f, "'{}'", s), // typically strings are quoted
            Value::Boolean(b) => write!(f, "{}", if *b { "TRUE" } else { "FALSE" }),
        }
    }
}

/// SQL operators
#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    // Comparison
    Equals,
    NotEquals,
    LessThan,
    GreaterThan,
    LessEquals,
    GreaterEquals,
    // Logical
    And,
    Or,
    Not,
    // Arithmetic
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Operator::Equals => write!(f, "="),
            Operator::NotEquals => write!(f, "!="),
            Operator::LessThan => write!(f, "<"),
            Operator::GreaterThan => write!(f, ">"),
            Operator::LessEquals => write!(f, "<="),
            Operator::GreaterEquals => write!(f, ">="),
            Operator::And => write!(f, "AND"),
            Operator::Or => write!(f, "OR"),
            Operator::Not => write!(f, "NOT"),
            Operator::Plus => write!(f, "+"),
            Operator::Minus => write!(f, "-"),
            Operator::Multiply => write!(f, "*"),
            Operator::Divide => write!(f, "/"),
            Operator::Modulo => write!(f, "%"),
        }
    }
}

impl Operator {
    /// Check if this operator is an equality operator (used for join type selection)
    pub fn is_equality(&self) -> bool {
        matches!(self, Operator::Equals)
    }
}

/// CREATE TABLE statement
#[derive(Debug, Clone)]
pub struct CreateStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

/// Column definition for CREATE TABLE
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
}

/// SQL data types
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Integer,
    Float,
    Text,
    Boolean,
    Date,
    Timestamp,
}

/// INSERT statement
#[derive(Debug, Clone)]
pub struct InsertStatement {
    pub table_name: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Expression>,
}

/// UPDATE statement
#[derive(Debug, Clone)]
pub struct UpdateStatement {
    pub table_name: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<Box<Expression>>,
}

/// Column assignment in UPDATE
#[derive(Debug, Clone)]
pub struct Assignment {
    pub column: String,
    pub value: Expression,
}

/// DELETE statement
#[derive(Debug, Clone)]
pub struct DeleteStatement {
    pub table_name: String,
    pub where_clause: Option<Box<Expression>>,
}

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Select(_) => write!(f, "SELECT statement"),
            Statement::Insert(_) => write!(f, "INSERT statement"),
            Statement::Update(_) => write!(f, "UPDATE statement"),
            Statement::Delete(_) => write!(f, "DELETE statement"),
            Statement::Create(_) => write!(f, "CREATE statement"),
        }
    }
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinType::Inner => write!(f, "INNER JOIN"),
            JoinType::LeftOuter => write!(f, "LEFT OUTER JOIN"),
            JoinType::RightOuter => write!(f, "RIGHT OUTER JOIN"),
            JoinType::FullOuter => write!(f, "FULL OUTER JOIN"),
            JoinType::Cross => write!(f, "CROSS JOIN"),
        }
    }
}

impl fmt::Display for AggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggregateFunction::Count => write!(f, "COUNT"),
            AggregateFunction::Sum => write!(f, "SUM"),
            AggregateFunction::Avg => write!(f, "AVG"),
            AggregateFunction::Min => write!(f, "MIN"),
            AggregateFunction::Max => write!(f, "MAX"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_ast() {
        // Build a simple SELECT statement AST
        let stmt = Statement::Select(SelectStatement {
            columns: vec![
                SelectColumn::Column(ColumnReference {
                    table: None,
                    name: "id".to_string(),
                }),
                SelectColumn::Column(ColumnReference {
                    table: None,
                    name: "name".to_string(),
                }),
            ],
            from: vec![TableReference {
                name: "users".to_string(),
                alias: None,
            }],
            where_clause: Some(Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column(ColumnReference {
                    table: None,
                    name: "id".to_string(),
                })),
                op: Operator::Equals,
                right: Box::new(Expression::Literal(Value::Integer(1))),
            })),
            joins: vec![],
            group_by: None,
            having: None,
        });

        // Verify it was constructed correctly
        if let Statement::Select(select) = stmt {
            assert_eq!(select.columns.len(), 2);
            assert_eq!(select.from.len(), 1);
            assert!(select.where_clause.is_some());
            assert!(select.joins.is_empty());
            assert!(select.group_by.is_none());
            assert!(select.having.is_none());
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_join_ast() {
        // Build a SELECT statement with a JOIN
        let stmt = Statement::Select(SelectStatement {
            columns: vec![
                SelectColumn::Column(ColumnReference {
                    table: Some("u".to_string()),
                    name: "id".to_string(),
                }),
                SelectColumn::Column(ColumnReference {
                    table: Some("u".to_string()),
                    name: "name".to_string(),
                }),
                SelectColumn::Column(ColumnReference {
                    table: Some("o".to_string()),
                    name: "order_id".to_string(),
                }),
            ],
            from: vec![TableReference {
                name: "users".to_string(),
                alias: Some("u".to_string()),
            }],
            where_clause: None,
            joins: vec![
                JoinClause {
                    join_type: JoinType::Inner,
                    table: TableReference {
                        name: "orders".to_string(),
                        alias: Some("o".to_string()),
                    },
                    condition: Box::new(Expression::BinaryOp {
                        left: Box::new(Expression::Column(ColumnReference {
                            table: Some("u".to_string()),
                            name: "id".to_string(),
                        })),
                        op: Operator::Equals,
                        right: Box::new(Expression::Column(ColumnReference {
                            table: Some("o".to_string()),
                            name: "user_id".to_string(),
                        })),
                    }),
                }
            ],
            group_by: None,
            having: None,
        });

        // Verify it was constructed correctly
        if let Statement::Select(select) = stmt {
            assert_eq!(select.columns.len(), 3);
            assert_eq!(select.from.len(), 1);
            assert_eq!(select.joins.len(), 1);
            
            let join = &select.joins[0];
            assert_eq!(join.join_type, JoinType::Inner);
            assert_eq!(join.table.name, "orders");
            assert_eq!(join.table.alias, Some("o".to_string()));
            
            // Verify JOIN condition
            match &*join.condition {
                Expression::BinaryOp { op, .. } => {
                    assert_eq!(*op, Operator::Equals);
                },
                _ => panic!("Expected binary operation in JOIN condition"),
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }
} 