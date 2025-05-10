// DML Statement Parser Implementation
//
// This module implements parsing for SQL DML (Data Manipulation Language) statements:
// INSERT, UPDATE, and DELETE

use crate::query::parser::ast::*;
use crate::query::parser::lexer::TokenType;
use super::parser_core::{Parser, ParseResult};
use super::parser_expressions::parse_expression;

/// Parse an INSERT statement
pub fn parse_insert(parser: &mut Parser) -> ParseResult<Statement> {
    // Consume INSERT keyword
    parser.expect_token(TokenType::INSERT)?;
    
    // Expect INTO keyword
    parser.expect_token(TokenType::INTO)?;
    
    // Parse table name
    let table_name = parser.parse_identifier()?;
    
    // Parse column list (optional)
    let columns = if parser.current_token_is(TokenType::LeftParen) {
        parser.next_token(); // Consume left paren
        
        let mut cols = Vec::new();
        
        // Parse first column
        cols.push(parser.parse_identifier()?);
        
        // Parse additional columns
        while parser.current_token_is(TokenType::COMMA) {
            parser.next_token(); // Consume comma
            cols.push(parser.parse_identifier()?);
        }
        
        // Expect closing parenthesis
        parser.expect_token(TokenType::RightParen)?;
        
        Some(cols)
    } else {
        None
    };
    
    // Expect VALUES keyword
    parser.expect_token(TokenType::VALUES)?;
    
    // Parse values list
    parser.expect_token(TokenType::LeftParen)?;
    
    let mut values = Vec::new();
    
    // Parse first value
    values.push(parse_expression(parser, 0)?);
    
    // Parse additional values
    while parser.current_token_is(TokenType::COMMA) {
        parser.next_token(); // Consume comma
        values.push(parse_expression(parser, 0)?);
    }
    
    // Expect closing parenthesis
    parser.expect_token(TokenType::RightParen)?;
    
    // Optional semicolon
    if parser.current_token_is(TokenType::SEMICOLON) {
        parser.next_token();
    }
    
    Ok(Statement::Insert(InsertStatement {
        table_name,
        columns,
        values,
    }))
}

/// Parse an UPDATE statement
pub fn parse_update(parser: &mut Parser) -> ParseResult<Statement> {
    // Consume UPDATE keyword
    parser.expect_token(TokenType::UPDATE)?;
    
    // Parse table name
    let table_name = parser.parse_identifier()?;
    
    // Expect SET keyword
    parser.expect_token(TokenType::SET)?;
    
    // Parse column assignments
    let mut assignments = Vec::new();
    
    // Parse first assignment
    let column = parser.parse_identifier()?;
    parser.expect_token(TokenType::EQUALS)?;
    let value = parse_expression(parser, 0)?;
    
    assignments.push(Assignment { column, value });
    
    // Parse additional assignments
    while parser.current_token_is(TokenType::COMMA) {
        parser.next_token(); // Consume comma
        
        let column = parser.parse_identifier()?;
        parser.expect_token(TokenType::EQUALS)?;
        let value = parse_expression(parser, 0)?;
        
        assignments.push(Assignment { column, value });
    }
    
    // Parse optional WHERE clause
    let where_clause = if parser.current_token_is(TokenType::WHERE) {
        parser.next_token(); // Consume WHERE
        Some(Box::new(parse_expression(parser, 0)?))
    } else {
        None
    };
    
    // Optional semicolon
    if parser.current_token_is(TokenType::SEMICOLON) {
        parser.next_token();
    }
    
    Ok(Statement::Update(UpdateStatement {
        table_name,
        assignments,
        where_clause,
    }))
}

/// Parse a DELETE statement
pub fn parse_delete(parser: &mut Parser) -> ParseResult<Statement> {
    // Consume DELETE keyword
    parser.expect_token(TokenType::DELETE)?;
    
    // Expect FROM keyword
    parser.expect_token(TokenType::FROM)?;
    
    // Parse table name
    let table_name = parser.parse_identifier()?;
    
    // Parse optional WHERE clause
    let where_clause = if parser.current_token_is(TokenType::WHERE) {
        parser.next_token(); // Consume WHERE
        Some(Box::new(parse_expression(parser, 0)?))
    } else {
        None
    };
    
    // Optional semicolon
    if parser.current_token_is(TokenType::SEMICOLON) {
        parser.next_token();
    }
    
    Ok(Statement::Delete(DeleteStatement {
        table_name,
        where_clause,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_insert() {
        let mut parser = Parser::new("INSERT INTO users (id, name) VALUES (1, 'John')");
        
        let stmt = parse_insert(&mut parser).unwrap();
        
        if let Statement::Insert(insert_stmt) = stmt {
            assert_eq!(insert_stmt.table_name, "users");
            assert_eq!(insert_stmt.columns.as_ref().unwrap().len(), 2);
            assert_eq!(insert_stmt.values.len(), 2);
            
            // Check columns
            let columns = insert_stmt.columns.unwrap();
            assert_eq!(columns[0], "id");
            assert_eq!(columns[1], "name");
            
            // Check values
            match &insert_stmt.values[0] {
                Expression::Literal(Value::Integer(val)) => assert_eq!(*val, 1),
                _ => panic!("Expected integer literal"),
            }
            
            match &insert_stmt.values[1] {
                Expression::Literal(Value::String(val)) => assert_eq!(val, "John"),
                _ => panic!("Expected string literal"),
            }
        } else {
            panic!("Expected INSERT statement");
        }
    }
    
    #[test]
    fn test_parse_update() {
        let mut parser = Parser::new("UPDATE users SET name = 'Jane', age = 30 WHERE id = 1");
        
        let stmt = parse_update(&mut parser).unwrap();
        
        if let Statement::Update(update_stmt) = stmt {
            assert_eq!(update_stmt.table_name, "users");
            assert_eq!(update_stmt.assignments.len(), 2);
            assert!(update_stmt.where_clause.is_some());
            
            // Check assignments
            assert_eq!(update_stmt.assignments[0].column, "name");
            assert_eq!(update_stmt.assignments[1].column, "age");
            
            // Check WHERE clause
            match update_stmt.where_clause.unwrap().as_ref() {
                Expression::BinaryOp { op, .. } => assert_eq!(*op, Operator::Equals),
                _ => panic!("Expected binary operation in WHERE clause"),
            }
        } else {
            panic!("Expected UPDATE statement");
        }
    }
    
    #[test]
    fn test_parse_delete() {
        let mut parser = Parser::new("DELETE FROM users WHERE id = 1");
        
        let stmt = parse_delete(&mut parser).unwrap();
        
        if let Statement::Delete(delete_stmt) = stmt {
            assert_eq!(delete_stmt.table_name, "users");
            assert!(delete_stmt.where_clause.is_some());
            
            // Check WHERE clause
            match delete_stmt.where_clause.unwrap().as_ref() {
                Expression::BinaryOp { op, .. } => assert_eq!(*op, Operator::Equals),
                _ => panic!("Expected binary operation in WHERE clause"),
            }
        } else {
            panic!("Expected DELETE statement");
        }
    }
} 