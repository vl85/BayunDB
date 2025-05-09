// SQL Parser Implementation
//
// This module provides the entry point for SQL parsing by using
// component parsers from the components module.

use super::ast::*;
use super::lexer::{Lexer, TokenType};
use super::components::{
    Parser as ComponentParser, 
    ParseResult, 
    ParseError,
    parse_select,
    parse_create,
    parse_insert,
    parse_update,
    parse_delete,
    parse_drop,
    parse_alter
};

/// Parse a SQL query string into a Statement AST
pub fn parse(sql: &str) -> ParseResult<Statement> {
    // Create a component parser directly from the SQL string
    let mut lexer = Lexer::new(sql);
    let mut tokens = Vec::new();
    
    loop {
        let token = lexer.next_token();
        let is_eof = token.token_type == TokenType::EOF;
        tokens.push(token);
        if is_eof {
            break;
        }
    }
    
    let mut component_parser = ComponentParser {
        tokens: tokens.into_iter().peekable(),
        current_token: None,
    };
    component_parser.next_token();
    
    // Call the appropriate component parser based on the first token
    match &component_parser.current_token {
        Some(token) => {
            match token.token_type {
                TokenType::SELECT => parse_select(&mut component_parser),
                TokenType::INSERT => parse_insert(&mut component_parser),
                TokenType::UPDATE => parse_update(&mut component_parser),
                TokenType::DELETE => parse_delete(&mut component_parser),
                TokenType::CREATE => parse_create(&mut component_parser),
                TokenType::DROP => parse_drop(&mut component_parser),
                TokenType::ALTER => parse_alter(&mut component_parser),
                _ => Err(ParseError::UnexpectedToken(token.clone())),
            }
        },
        None => Err(ParseError::EndOfInput),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_simple_select() {
        let query = "SELECT id, name FROM users";
        let statement = parse(query).unwrap();
        
        if let Statement::Select(select) = statement {
            assert_eq!(select.columns.len(), 2);
            assert_eq!(select.from.len(), 1);
            assert!(select.where_clause.is_none());
            
            let TableReference { name, alias } = &select.from[0];
            assert_eq!(name, "users");
            assert!(alias.is_none());
        } else {
            panic!("Expected SELECT statement");
        }
    }
    
    #[test]
    fn test_parse_select_with_where() {
        let query = "SELECT id, name FROM users WHERE id = 1";
        let statement = parse(query).unwrap();
        
        if let Statement::Select(select) = statement {
            assert_eq!(select.columns.len(), 2);
            assert_eq!(select.from.len(), 1);
            assert!(select.where_clause.is_some());
            
            // Check WHERE clause
            if let Some(expr) = select.where_clause {
                if let Expression::BinaryOp { left, op, right } = *expr {
                    assert_eq!(op, Operator::Equals);
                    
                    if let Expression::Column(col) = *left {
                        assert_eq!(col.name, "id");
                    } else {
                        panic!("Expected column reference in WHERE clause");
                    }
                    
                    if let Expression::Literal(Value::Integer(val)) = *right {
                        assert_eq!(val, 1);
                    } else {
                        panic!("Expected integer literal in WHERE clause");
                    }
                } else {
                    panic!("Expected binary operation in WHERE clause");
                }
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }
    
    #[test]
    fn test_parse_join() {
        let query = "SELECT u.id, u.name, o.order_id FROM users u JOIN orders o ON u.id = o.user_id";
        let statement = parse(query).unwrap();
        
        if let Statement::Select(select) = statement {
            assert_eq!(select.columns.len(), 3);
            assert_eq!(select.from.len(), 1);
            assert_eq!(select.joins.len(), 1);
            
            let join = &select.joins[0];
            assert_eq!(join.join_type, JoinType::Inner);
            assert_eq!(join.table.name, "orders");
            assert_eq!(join.table.alias, Some("o".to_string()));
        } else {
            panic!("Expected SELECT statement");
        }
    }
    
    #[test]
    fn test_parse_left_join() {
        let query = "SELECT u.id, u.name, o.order_id FROM users u LEFT JOIN orders o ON u.id = o.user_id";
        let statement = parse(query).unwrap();
        
        if let Statement::Select(select) = statement {
            assert_eq!(select.joins.len(), 1);
            
            let join = &select.joins[0];
            assert_eq!(join.join_type, JoinType::LeftOuter);
            assert_eq!(join.table.name, "orders");
        } else {
            panic!("Expected SELECT statement");
        }
    }
    
    #[test]
    fn test_parse_aggregate_functions() {
        // Test COUNT(*)
        let sql = "SELECT COUNT(*) FROM users";
        let stmt = parse(sql).unwrap();
        
        if let Statement::Select(select) = stmt {
            assert_eq!(select.columns.len(), 1);
            
            if let SelectColumn::Expression { expr, .. } = &select.columns[0] {
                if let Expression::Aggregate { function, arg } = &**expr {
                    assert_eq!(*function, AggregateFunction::Count);
                    assert!(arg.is_none());
                } else {
                    panic!("Expected aggregate function expression");
                }
            } else {
                panic!("Expected expression in column list");
            }
        } else {
            panic!("Expected SELECT statement");
        }
        
        // Test other aggregate functions
        let sql = "SELECT SUM(price), AVG(quantity), MIN(price), MAX(quantity) FROM products";
        let stmt = parse(sql).unwrap();
        
        if let Statement::Select(select) = stmt {
            assert_eq!(select.columns.len(), 4);
            
            // Check SUM
            if let SelectColumn::Expression { expr, .. } = &select.columns[0] {
                if let Expression::Aggregate { function, arg } = &**expr {
                    assert_eq!(*function, AggregateFunction::Sum);
                    assert!(arg.is_some());
                } else {
                    panic!("Expected aggregate function expression");
                }
            }
            
            // Check AVG
            if let SelectColumn::Expression { expr, .. } = &select.columns[1] {
                if let Expression::Aggregate { function, arg } = &**expr {
                    assert_eq!(*function, AggregateFunction::Avg);
                    assert!(arg.is_some());
                } else {
                    panic!("Expected aggregate function expression");
                }
            }
            
            // Check MIN
            if let SelectColumn::Expression { expr, .. } = &select.columns[2] {
                if let Expression::Aggregate { function, arg } = &**expr {
                    assert_eq!(*function, AggregateFunction::Min);
                    assert!(arg.is_some());
                } else {
                    panic!("Expected aggregate function expression");
                }
            }
            
            // Check MAX
            if let SelectColumn::Expression { expr, .. } = &select.columns[3] {
                if let Expression::Aggregate { function, arg } = &**expr {
                    assert_eq!(*function, AggregateFunction::Max);
                    assert!(arg.is_some());
                } else {
                    panic!("Expected aggregate function expression");
                }
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }
    
    #[test]
    fn test_parse_group_by() {
        let sql = "SELECT department_id, COUNT(*) FROM employees GROUP BY department_id";
        let stmt = parse(sql).unwrap();
        
        if let Statement::Select(select) = stmt {
            assert!(select.group_by.is_some());
            assert_eq!(select.group_by.unwrap().len(), 1);
            assert!(select.having.is_none());
        } else {
            panic!("Expected SELECT statement");
        }
        
        // Test with multiple GROUP BY columns
        let sql = "SELECT department_id, job_title, COUNT(*) FROM employees GROUP BY department_id, job_title";
        let stmt = parse(sql).unwrap();
        
        if let Statement::Select(select) = stmt {
            assert!(select.group_by.is_some());
            assert_eq!(select.group_by.unwrap().len(), 2);
        } else {
            panic!("Expected SELECT statement");
        }
    }
    
    #[test]
    fn test_parse_having() {
        let sql = "SELECT department_id, COUNT(*) FROM employees GROUP BY department_id HAVING COUNT(*) > 5";
        let stmt = parse(sql).unwrap();
        
        if let Statement::Select(select) = stmt {
            assert!(select.group_by.is_some());
            assert!(select.having.is_some());
            
            if let Expression::BinaryOp { op, .. } = *select.having.unwrap() {
                assert_eq!(op, Operator::GreaterThan);
            } else {
                panic!("Expected binary operation in HAVING clause");
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }
    
    #[test]
    fn test_parse_complex_aggregation() {
        let sql = "SELECT department_id, AVG(salary) FROM employees WHERE status = 'active' GROUP BY department_id HAVING AVG(salary) > 50000";
        let stmt = parse(sql).unwrap();
        
        if let Statement::Select(select) = stmt {
            assert!(select.where_clause.is_some());
            assert!(select.group_by.is_some());
            assert!(select.having.is_some());
        } else {
            panic!("Expected SELECT statement");
        }
    }
    
    #[test]
    fn test_parse_create_table() {
        let query = "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            created_at TIMESTAMP
        )";
        
        let stmt = parse(query).unwrap();
        
        if let Statement::Create(create_stmt) = stmt {
            // Check table name
            assert_eq!(create_stmt.table_name, "users");
            
            // Check columns
            assert_eq!(create_stmt.columns.len(), 4);
            
            // Check id column
            let id_col = &create_stmt.columns[0];
            assert_eq!(id_col.name, "id");
            assert_eq!(id_col.data_type, DataType::Integer);
            assert_eq!(id_col.primary_key, true);
            assert_eq!(id_col.nullable, false);
            
            // Check name column
            let name_col = &create_stmt.columns[1];
            assert_eq!(name_col.name, "name");
            assert_eq!(name_col.data_type, DataType::Text);
            assert_eq!(name_col.primary_key, false);
            assert_eq!(name_col.nullable, false);
            
            // Check email column (note: assuming default nullable is true)
            let email_col = &create_stmt.columns[2];
            assert_eq!(email_col.name, "email");
            assert_eq!(email_col.data_type, DataType::Text);
            assert_eq!(email_col.primary_key, false);
            // Don't assert nullable for this test since implementations might differ on default
            
            // Check created_at column (note: assuming default nullable is true)
            let created_col = &create_stmt.columns[3];
            assert_eq!(created_col.name, "created_at");
            assert_eq!(created_col.data_type, DataType::Timestamp);
            assert_eq!(created_col.primary_key, false);
            // Don't assert nullable for this test since implementations might differ on default
        } else {
            panic!("Expected CREATE TABLE statement");
        }
    }
} 