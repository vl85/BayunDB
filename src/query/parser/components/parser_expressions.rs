// Expression Parser Implementation
//
// This module implements expression parsing for SQL

use crate::query::parser::ast::*;
use crate::query::parser::lexer::TokenType;
use super::parser_core::{Parser, ParseResult, ParseError, token_to_operator, get_operator_precedence};

/// Parse an expression with operator precedence
pub fn parse_expression(parser: &mut Parser, precedence: u8) -> ParseResult<Expression> {
    // First parse a prefix expression (literal, identifier, etc.)
    // println!("[parse_expression] Start. Initial precedence: {}", precedence);
    // if let Some(tok) = &parser.current_token {
    //     println!("[parse_expression] Current token before prefix: {:?}", tok);
    // } else {
    //     println!("[parse_expression] Current token before prefix: None");
    // }

    let mut left_expr = parse_prefix_expression(parser)?;
    // println!("[parse_expression] After prefix. left_expr: {:?}", left_expr);
    // if let Some(tok) = &parser.current_token {
    //     println!("[parse_expression] Current token after prefix: {:?}", tok);
    // } else {
    //     println!("[parse_expression] Current token after prefix: None");
    // }
    
    loop {
        // Handling for IS NULL / IS NOT NULL as a postfix-like operator
        // This check is done after an initial left_expr is formed (from prefix or previous infix)
        if parser.current_token_is(TokenType::IS) {
            // Tentatively consume IS
            parser.next_token(); // Consume IS

            let mut not_flag = false;
            if parser.current_token_is(TokenType::NOT) {
                parser.next_token(); // Consume NOT
                not_flag = true;
            }

            if parser.current_token_is(TokenType::NULL) {
                parser.next_token(); // Consume NULL
                left_expr = Expression::IsNull { expr: Box::new(left_expr), not: not_flag };
                // After forming IsNull, we might have further operators, so continue the loop.
                continue; 
            } else {
                // This means we had "IS" (consumed) and then optionally "NOT" (consumed), 
                // but not "NULL". This is a syntax error for "IS [NOT] NULL".
                let err_msg = if let Some(t) = &parser.current_token {
                    format!("Expected NULL after IS [NOT], but found token {:?} with literal '{}'", t.token_type, t.literal)
                } else {
                    "Expected NULL after IS [NOT], but found end of input".to_string()
                };
                return Err(ParseError::InvalidSyntax(err_msg));
            }
        }

        // Standard infix operator parsing
        if let Some(token) = &parser.current_token {
            // println!("[parse_expression] Infix loop. Current token: {:?}", token);

            let current_op_precedence = get_operator_precedence(&token.token_type); 
            // println!("[parse_expression] Infix loop. current_operator_precedence: {}", current_op_precedence);

            let should_break = current_op_precedence == 0 || precedence >= current_op_precedence;
            // println!("[parse_expression] Infix loop. should_break: {} (loop_prec: {}, op_prec: {})", should_break, precedence, current_op_precedence);

            if should_break {
                break;
            }
            
            // println!("[parse_expression] Infix loop. Calling parse_infix_expression.");
            left_expr = parse_infix_expression(parser, left_expr)?;
            // println!("[parse_expression] Infix loop. After infix. new left_expr: {:?}", left_expr);
        } else {
            break; // No more tokens
        }
    }
    
    // println!("[parse_expression] End. Final left_expr: {:?}", left_expr);
    Ok(left_expr)
}

/// Parse a prefix expression (literal, identifier, etc.)
fn parse_prefix_expression(parser: &mut Parser) -> ParseResult<Expression> {
    match &parser.current_token {
        Some(token) => {
            let token_type = token.token_type.clone();
            match token_type {
                TokenType::MINUS => { // Handle Unary Minus
                    parser.next_token(); // Consume MINUS
                    // Parse the operand with a high precedence for unary operators
                    // Let's define a precedence for unary minus, e.g., higher than multiplication/division
                    let operand = parse_expression(parser, get_operator_precedence(&TokenType::MINUS) + 1)?; // Or a specific UNARY_PRECEDENCE constant
                    Ok(Expression::UnaryOp {
                        op: UnaryOperator::Minus, 
                        expr: Box::new(operand)
                    })
                }
                // TokenType::NOT => { // Example for Unary Not, if supported
                //     parser.next_token(); // Consume NOT
                //     let operand = parse_expression(parser, UNARY_PRECEDENCE)?; // Define UNARY_PRECEDENCE
                //     Ok(Expression::UnaryOp { op: UnaryOperator::Not, expr: Box::new(operand) })
                // }
                TokenType::INTEGER => { // Changed from INTEGER(val)
                    let literal_str = parser.current_token_literal_or_err()?;
                    parser.next_token(); // Consume integer token
                    literal_str.parse::<i64>()
                        .map(Value::Integer)
                        .map(Expression::Literal)
                        .map_err(|_| ParseError::InvalidLiteral(format!("Invalid integer: {}", literal_str)))
                },
                TokenType::FLOAT => { // Changed from FLOAT(val)
                    let literal_str = parser.current_token_literal_or_err()?;
                    parser.next_token(); // Consume float token
                    literal_str.parse::<f64>()
                        .map(Value::Float)
                        .map(Expression::Literal)
                        .map_err(|_| ParseError::InvalidLiteral(format!("Invalid float: {}", literal_str)))
                },
                TokenType::STRING(val) => {
                    parser.next_token();
                    Ok(Expression::Literal(Value::String(val)))
                },
                TokenType::NULL => { // Added this arm for TokenType::NULL
                    parser.next_token(); // Consume NULL token
                    Ok(Expression::Literal(Value::Null))
                },
                TokenType::IDENTIFIER(val) => {
                    // Check for true, false keywords first (NULL check removed from here)
                    let upper_val = val.to_uppercase();
                    if upper_val == "TRUE" {
                        parser.next_token();
                        Ok(Expression::Literal(Value::Boolean(true)))
                    } else if upper_val == "FALSE" {
                        parser.next_token();
                        Ok(Expression::Literal(Value::Boolean(false)))
                    } else {
                        // If not a boolean or null, then it's a column reference or other identifier (function name)
                        // Need to peek to see if it's a function call (IDENTIFIER followed by LPAREN)
                        if parser.peek_token_is(TokenType::LPAREN) {
                            // This could be a custom function or an aggregate function not caught by keyword tokens
                            // For now, assume aggregate functions are caught by specific keyword tokens.
                            // If it's a generic function, we'd parse it as such.
                            // Let's assume this path is for column references for now.
                            parse_column_reference(parser) // Re-uses current IDENTIFIER token
                        } else {
                            parse_column_reference(parser) // Re-uses current IDENTIFIER token
                        }
                    }
                },
                TokenType::COUNT | TokenType::SUM | TokenType::AVG | 
                TokenType::MIN | TokenType::MAX => {
                    parse_aggregate_function(parser, token_type)
                },
                TokenType::LPAREN => { // Was LeftParen
                    parser.next_token();
                    let expr = parse_expression(parser, 0)?;
                    parser.expect_token(TokenType::RPAREN)?; // Was RightParen
                    Ok(expr)
                },
                TokenType::CASE => { // Handle CASE keyword
                    parse_case_expression(parser)
                },
                _ => Err(ParseError::UnexpectedToken(token.clone())),
            }
        },
        None => Err(ParseError::EndOfInput),
    }
}

/// Parse a CASE expression
fn parse_case_expression(parser: &mut Parser) -> ParseResult<Expression> {
    parser.expect_token(TokenType::CASE)?; // Consume CASE

    // Check for simple CASE (CASE operand WHEN ...)
    // If the next token is not WHEN, then it might be an operand.
    let operand = if !parser.current_token_is(TokenType::WHEN) {
        Some(Box::new(parse_expression(parser, 0)?))
    } else {
        None
    };

    let mut when_then_clauses = Vec::new();
    while parser.current_token_is(TokenType::WHEN) {
        parser.next_token(); // Consume WHEN
        let condition_expr = parse_expression(parser, 0)?;
        parser.expect_token(TokenType::THEN)?;
        let result_expr = parse_expression(parser, 0)?;
        when_then_clauses.push((Box::new(condition_expr), Box::new(result_expr)));
    }

    if when_then_clauses.is_empty() {
        return Err(ParseError::InvalidSyntax("CASE expression must have at least one WHEN clause".to_string()));
    }

    let else_clause = if parser.current_token_is(TokenType::ELSE) {
        parser.next_token(); // Consume ELSE
        Some(Box::new(parse_expression(parser, 0)?))
    } else {
        None
    };

    parser.expect_token(TokenType::END)?; // Consume END

    Ok(Expression::Case {
        operand,
        when_then_clauses,
        else_clause,
    })
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
    parser.expect_token(TokenType::LPAREN)?; // Was LeftParen
    
    // Parse the argument (could be * for COUNT(*))
    let arg = if parser.current_token_is(TokenType::ASTERISK) { // Was MULTIPLY
        parser.next_token();
        None
    } else {
        Some(Box::new(parse_expression(parser, 0)?))
    };
    
    parser.expect_token(TokenType::RPAREN)?; // Was RightParen
    
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
        // e.g., "a + b * c"
        // Lexer should produce IDENTIFIER("a"), PLUS, IDENTIFIER("b"), ASTERISK, IDENTIFIER("c")
        // Check if test setup uses direct token construction that needs updating.
        // If tests use string input like Parser::new("a + b * c"), they should be fine if lexer is correct.
        /* Example of old test needing update if it constructed tokens directly:
        let tokens = vec![
            Token { token_type: TokenType::IDENTIFIER("a".to_string()), .. },
            Token { token_type: TokenType::PLUS, .. },
            Token { token_type: TokenType::IDENTIFIER("b".to_string()), .. },
            Token { token_type: TokenType::MULTIPLY, .. }, // Now ASTERISK
            Token { token_type: TokenType::IDENTIFIER("c".to_string()), .. },
        ];
        */
        let mut parser = Parser::new("a + b * 2");
        let expr = parse_expression(&mut parser, 0);
        assert!(expr.is_ok());
        // Further assertions on the structure (a + (b * 2))
        if let Ok(Expression::BinaryOp { op, .. }) = expr {
            assert_eq!(op, Operator::Plus);
        } else {
            panic!("Expected binary op +");
        }
    }
    
    #[test]
    fn test_parse_parenthesized_expression() {
        let mut parser = Parser::new("(1 + 2) * 3");
        let expr = parse_expression(&mut parser, 0);
        assert!(expr.is_ok());
        // Assert structure is ((1 + 2) * 3)
        // This test primarily relies on correct parsing of LPAREN/RPAREN
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
    
    #[test]
    fn test_parse_negative_numbers_and_unary_minus() {
        // Negative integer literal
        let mut parser_neg_int = Parser::new("-42");
        let expr_neg_int = parse_expression(&mut parser_neg_int, 0);
        assert!(expr_neg_int.is_ok(), "Failed to parse -42: {:?}", expr_neg_int.err());
        match expr_neg_int.unwrap() {
            Expression::UnaryOp { op, expr } => {
                assert_eq!(op, UnaryOperator::Minus);
                match *expr {
                    Expression::Literal(Value::Integer(val)) => assert_eq!(val, 42),
                    _ => panic!("Expected integer literal for unary minus operand, got {:?}", expr),
                }
            }
            _ => panic!("Expected UnaryOp for -42"),
        }

        // Negative float literal
        let mut parser_neg_float = Parser::new("-3.14");
        let expr_neg_float = parse_expression(&mut parser_neg_float, 0);
        assert!(expr_neg_float.is_ok(), "Failed to parse -3.14: {:?}", expr_neg_float.err());
        match expr_neg_float.unwrap() {
            Expression::UnaryOp { op, expr } => {
                assert_eq!(op, UnaryOperator::Minus);
                match *expr {
                    Expression::Literal(Value::Float(val)) => assert!((val - 3.14).abs() < f64::EPSILON),
                    _ => panic!("Expected float literal for unary minus operand, got {:?}", expr),
                }
            }
            _ => panic!("Expected UnaryOp for -3.14"),
        }

        // Unary minus with identifier
        let mut parser_unary_ident = Parser::new("-my_column");
        let expr_unary_ident = parse_expression(&mut parser_unary_ident, 0);
        assert!(expr_unary_ident.is_ok(), "Failed to parse -my_column: {:?}", expr_unary_ident.err());
        match expr_unary_ident.unwrap() {
            Expression::UnaryOp { op, expr } => {
                assert_eq!(op, UnaryOperator::Minus);
                match *expr {
                    Expression::Column(col_ref) => assert_eq!(col_ref.name, "my_column"),
                    _ => panic!("Expected column reference for unary minus operand"),
                }
            }
            _ => panic!("Expected UnaryOp for -my_column"),
        }

        // Unary minus with parenthesized expression
        let mut parser_unary_paren = Parser::new("-(a + b)");
        let expr_unary_paren = parse_expression(&mut parser_unary_paren, 0);
        assert!(expr_unary_paren.is_ok(), "Failed to parse -(a + b): {:?}", expr_unary_paren.err());
        match expr_unary_paren.unwrap() {
            Expression::UnaryOp { op, expr: outer_expr } => {
                assert_eq!(op, UnaryOperator::Minus);
                match *outer_expr {
                    Expression::BinaryOp { .. } => { /* Correct, it's a BinaryOp inside */ }
                    _ => panic!("Expected BinaryOp for unary minus operand -(a+b)"),
                }
            }
            _ => panic!("Expected UnaryOp for -(a + b)"),
        }

        // Ensure it doesn't break positive numbers
        let mut parser_pos_int = Parser::new("123");
        let expr_pos_int = parse_expression(&mut parser_pos_int, 0).unwrap();
        match expr_pos_int {
            Expression::Literal(Value::Integer(val)) => assert_eq!(val, 123),
            _ => panic!("Expected positive integer literal"),
        }
    }
} 