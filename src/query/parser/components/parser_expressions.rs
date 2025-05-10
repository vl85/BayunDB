// Expression Parser Implementation
//
// This module implements expression parsing for SQL

use crate::query::parser::ast::*;
use crate::query::parser::lexer::TokenType;
use super::parser_core::{Parser, ParseResult, ParseError, token_to_operator, get_operator_precedence};

/// Parse an expression with operator precedence
pub fn parse_expression(parser: &mut Parser, precedence: u8) -> ParseResult<Expression> {
    // First parse a prefix expression (literal, identifier, etc.)
    let mut left_expr = parse_prefix_expression(parser)?;
    
    // Then handle infix expressions based on precedence
    while let Some(token) = &parser.current_token {
        // Check if the current token can be an infix operator
        // We get precedence based on TokenType directly as per parser_core.rs definition
        let current_precedence = get_operator_precedence(&token.token_type); 

        // If the token is not an operator or its precedence is too low, stop.
        // A precedence of 0 typically means it's not an operator we handle here.
        if current_precedence == 0 || precedence >= current_precedence {
            break;
        }
        
        // If we are here, it means current_token is an operator 
        // whose precedence allows it to be processed.
        // parse_infix_expression will handle the specific operator logic.
        left_expr = parse_infix_expression(parser, left_expr)?;
    }
    
    Ok(left_expr)
}

/// Parse a prefix expression (literal, identifier, etc.)
fn parse_prefix_expression(parser: &mut Parser) -> ParseResult<Expression> {
    match &parser.current_token {
        Some(token) => {
            let token_type = token.token_type.clone();
            match token_type {
                TokenType::INTEGER(val) => {
                    parser.next_token();
                    Ok(Expression::Literal(Value::Integer(val)))
                },
                TokenType::FLOAT(val) => {
                    parser.next_token();
                    Ok(Expression::Literal(Value::Float(val)))
                },
                TokenType::STRING(val) => {
                    parser.next_token();
                    Ok(Expression::Literal(Value::String(val)))
                },
                TokenType::IDENTIFIER(val) => {
                    // Check for boolean literals first
                    if val.eq_ignore_ascii_case("true") {
                        parser.next_token();
                        Ok(Expression::Literal(Value::Boolean(true)))
                    } else if val.eq_ignore_ascii_case("false") {
                        parser.next_token();
                        Ok(Expression::Literal(Value::Boolean(false)))
                    } else {
                        // If not a boolean, then it's a column reference or other identifier
                        parse_column_reference(parser)
                    }
                },
                TokenType::COUNT | TokenType::SUM | TokenType::AVG | 
                TokenType::MIN | TokenType::MAX => {
                    parse_aggregate_function(parser, token_type)
                },
                TokenType::LeftParen => {
                    parser.next_token(); // Consume left paren
                    let expr = parse_expression(parser, 0)?;
                    parser.expect_token(TokenType::RightParen)?; // Expect right paren
                    Ok(expr)
                },
                _ => Err(ParseError::UnexpectedToken(token.clone())),
            }
        },
        None => Err(ParseError::EndOfInput),
    }
}

/// Parse an infix expression (binary operations)
fn parse_infix_expression(parser: &mut Parser, left: Expression) -> ParseResult<Expression> {
    match &parser.current_token {
        Some(token) => {
            // Get precedence from TokenType for the current operator
            let op_token_type = token.token_type.clone();
            let op_precedence = get_operator_precedence(&op_token_type);

            // Convert TokenType to the ast::Operator enum for the expression node
            let ast_op = token_to_operator(&op_token_type)?;
            
            parser.next_token(); // Consume the operator token
            
            // Parse the right-hand side with the current operator's precedence
            let right = parse_expression(parser, op_precedence)?;
            
            Ok(Expression::BinaryOp {
                left: Box::new(left),
                op: ast_op, // Use the converted ast::Operator here
                right: Box::new(right),
            })
        }
        None => Err(ParseError::EndOfInput),
    }
}

/// Parse a column reference (possibly qualified with table name)
fn parse_column_reference(parser: &mut Parser) -> ParseResult<Expression> {
    match &parser.current_token {
        Some(token) => {
            if let TokenType::IDENTIFIER(name) = &token.token_type {
                let name = name.clone();
                parser.next_token();
                
                // Check if we have a dot for a qualified column
                if parser.current_token_is(TokenType::DOT) {
                    parser.next_token(); // Consume dot
                    
                    match &parser.current_token {
                        Some(token) => {
                            if let TokenType::IDENTIFIER(col_name) = &token.token_type {
                                let col_name = col_name.clone();
                                parser.next_token();
                                Ok(Expression::Column(ColumnReference {
                                    table: Some(name),
                                    name: col_name,
                                }))
                            } else {
                                Err(ParseError::UnexpectedToken(token.clone()))
                            }
                        },
                        None => Err(ParseError::EndOfInput),
                    }
                } else {
                    Ok(Expression::Column(ColumnReference {
                        table: None,
                        name,
                    }))
                }
            } else {
                Err(ParseError::UnexpectedToken(token.clone()))
            }
        },
        None => Err(ParseError::EndOfInput),
    }
}

/// Parse an aggregate function expression
fn parse_aggregate_function(parser: &mut Parser, token_type: TokenType) -> ParseResult<Expression> {
    // Convert token to aggregate function type
    let function = match token_type {
        TokenType::COUNT => AggregateFunction::Count,
        TokenType::SUM => AggregateFunction::Sum,
        TokenType::AVG => AggregateFunction::Avg,
        TokenType::MIN => AggregateFunction::Min,
        TokenType::MAX => AggregateFunction::Max,
        _ => return Err(ParseError::InvalidSyntax("Expected aggregate function".to_string())),
    };
    
    // Consume the function name
    parser.next_token();
    
    // Expect opening parenthesis
    parser.expect_token(TokenType::LeftParen)?;
    
    // Parse the argument (could be * for COUNT(*))
    let arg = if parser.current_token_is(TokenType::MULTIPLY) {
        parser.next_token(); // Consume *
        None
    } else {
        Some(Box::new(parse_expression(parser, 0)?))
    };
    
    // Expect closing parenthesis
    parser.expect_token(TokenType::RightParen)?;
    
    Ok(Expression::Aggregate { function, arg })
}

/* // Commenting out unused function
fn parse_function_call(parser: &mut Parser, function_name: String) -> ParseResult<Expression> {
    // Implementation for parsing function calls, e.g., COUNT(*), SUBSTRING(col, 1, 3)
    // This would involve parsing arguments within parentheses
    // For now, return a placeholder or error
    Err(ParseError::NotYetImplemented("Function call parsing".to_string()))
}
*/

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_literal() {
        // Integer literal
        let mut parser = Parser::new("42");
        let expr = parse_expression(&mut parser, 0);
        assert!(expr.is_ok());
        
        match expr.unwrap() {
            Expression::Literal(Value::Integer(val)) => assert_eq!(val, 42),
            _ => panic!("Expected integer literal"),
        }
        
        // Float literal
        let mut parser = Parser::new("3.14");
        let expr = parse_expression(&mut parser, 0);
        assert!(expr.is_ok());
        
        match expr.unwrap() {
            Expression::Literal(Value::Float(val)) => assert!((val - 3.14).abs() < f64::EPSILON),
            _ => panic!("Expected float literal"),
        }
        
        // String literal
        let mut parser = Parser::new("'hello'");
        let expr = parse_expression(&mut parser, 0);
        assert!(expr.is_ok());
        
        match expr.unwrap() {
            Expression::Literal(Value::String(val)) => assert_eq!(val, "hello"),
            _ => panic!("Expected string literal"),
        }
    }
    
    #[test]
    fn test_parse_column_reference() {
        // Simple column reference
        let mut parser = Parser::new("column_name");
        let expr = parse_expression(&mut parser, 0);
        assert!(expr.is_ok());
        
        match expr.unwrap() {
            Expression::Column(col) => {
                assert_eq!(col.name, "column_name");
                assert!(col.table.is_none());
            },
            _ => panic!("Expected column reference"),
        }
        
        // Qualified column reference
        let mut parser = Parser::new("table_name.column_name");
        let expr = parse_expression(&mut parser, 0);
        assert!(expr.is_ok());
        
        match expr.unwrap() {
            Expression::Column(col) => {
                assert_eq!(col.name, "column_name");
                assert_eq!(col.table, Some("table_name".to_string()));
            },
            _ => panic!("Expected qualified column reference"),
        }
    }
    
    #[test]
    fn test_parse_binary_expression() {
        // Simple addition
        let mut parser = Parser::new("a + b");
        let expr = parse_expression(&mut parser, 0);
        assert!(expr.is_ok());
        
        match expr.unwrap() {
            Expression::BinaryOp { op, .. } => {
                assert_eq!(op, Operator::Plus);
            },
            _ => panic!("Expected binary operation"),
        }
        
        // Comparison with precedence
        let mut parser = Parser::new("a + b * c");
        let expr = parse_expression(&mut parser, 0);
        assert!(expr.is_ok());
        
        match expr.unwrap() {
            Expression::BinaryOp { left, op, right } => {
                assert_eq!(op, Operator::Plus);
                
                // Left should be a simple column
                match *left {
                    Expression::Column(col) => {
                        assert_eq!(col.name, "a");
                    },
                    _ => panic!("Expected column reference for left side"),
                }
                
                // Right should be a binary operation (b * c)
                match *right {
                    Expression::BinaryOp { op, .. } => {
                        assert_eq!(op, Operator::Multiply);
                    },
                    _ => panic!("Expected binary operation for right side"),
                }
            },
            _ => panic!("Expected binary operation"),
        }
    }
    
    #[test]
    fn test_parse_parenthesized_expression() {
        let mut parser = Parser::new("(a + b) * c");
        let expr = parse_expression(&mut parser, 0);
        assert!(expr.is_ok());
        
        match expr.unwrap() {
            Expression::BinaryOp { left, op, right } => {
                assert_eq!(op, Operator::Multiply);
                
                // Left should be a binary operation (a + b)
                match *left {
                    Expression::BinaryOp { op, .. } => {
                        assert_eq!(op, Operator::Plus);
                    },
                    _ => panic!("Expected binary operation for left side"),
                }
                
                // Right should be a simple column
                match *right {
                    Expression::Column(col) => {
                        assert_eq!(col.name, "c");
                    },
                    _ => panic!("Expected column reference for right side"),
                }
            },
            _ => panic!("Expected binary operation"),
        }
    }
    
    #[test]
    fn test_parse_aggregate_function() {
        // COUNT(*)
        let mut parser = Parser::new("COUNT(*)");
        let expr = parse_expression(&mut parser, 0);
        assert!(expr.is_ok());
        
        match expr.unwrap() {
            Expression::Aggregate { function, arg } => {
                assert_eq!(function, AggregateFunction::Count);
                assert!(arg.is_none());
            },
            _ => panic!("Expected aggregate function"),
        }
        
        // SUM(column_name)
        let mut parser = Parser::new("SUM(column_name)");
        let expr = parse_expression(&mut parser, 0);
        assert!(expr.is_ok());
        
        match expr.unwrap() {
            Expression::Aggregate { function, arg } => {
                assert_eq!(function, AggregateFunction::Sum);
                assert!(arg.is_some());
            },
            _ => panic!("Expected aggregate function"),
        }
    }
} 