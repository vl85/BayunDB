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
#[derive(Debug, Clone)]
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
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expression>>,
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
        });

        // Verify it was constructed correctly
        if let Statement::Select(select) = stmt {
            assert_eq!(select.columns.len(), 2);
            assert_eq!(select.from.len(), 1);
            assert!(select.where_clause.is_some());
        } else {
            panic!("Expected SELECT statement");
        }
    }
} 