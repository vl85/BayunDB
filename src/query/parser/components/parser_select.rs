// SELECT Statement Parser Implementation
//
// This module implements parsing for SQL SELECT statements

use crate::query::parser::ast::*;
use crate::query::parser::lexer::TokenType;
use super::parser_core::{Parser, ParseResult, ParseError};
use super::parser_expressions::parse_expression;

/// Parse a SELECT statement
pub fn parse_select(parser: &mut Parser) -> ParseResult<Statement> {
    // Consume SELECT keyword
    parser.expect_token(TokenType::SELECT)?;
    
    // Parse columns
    let columns = parse_select_columns(parser)?;
    
    // Expect FROM keyword
    parser.expect_token(TokenType::FROM)?;
    
    // Parse FROM clause
    let from = parse_table_references(parser)?;
    
    // Parse optional JOIN clauses
    let mut joins = Vec::new();
    while parser.current_token_is(TokenType::JOIN) || 
          parser.current_token_is(TokenType::INNER) ||
          parser.current_token_is(TokenType::LEFT) ||
          parser.current_token_is(TokenType::RIGHT) ||
          parser.current_token_is(TokenType::FULL) ||
          parser.current_token_is(TokenType::CROSS) {
        joins.push(parse_join_clause(parser)?);
    }
    
    // Parse optional WHERE clause
    let where_clause = if parser.current_token_is(TokenType::WHERE) {
        parser.next_token(); // Consume WHERE
        Some(Box::new(parse_expression(parser, 0)?))
    } else {
        None
    };
    
    // Parse optional GROUP BY clause
    let group_by = if parser.current_token_is(TokenType::GROUP) {
        parser.next_token(); // Consume GROUP
        parser.expect_token(TokenType::BY)?; // Expect BY
        Some(parse_group_by_expressions(parser)?)
    } else {
        None
    };
    
    // Parse optional HAVING clause
    let having = if parser.current_token_is(TokenType::HAVING) {
        parser.next_token(); // Consume HAVING
        Some(Box::new(parse_expression(parser, 0)?))
    } else {
        None
    };
    
    // Parse optional ORDER BY clause
    let order_by = if parser.current_token_is(TokenType::ORDER) {
        parse_order_by_clause(parser)?
    } else {
        Vec::new()
    };
    
    // Optional semicolon at the end
    if parser.current_token_is(TokenType::SEMICOLON) {
        parser.next_token();
    }
    
    Ok(Statement::Select(SelectStatement {
        columns,
        from,
        where_clause,
        joins,
        group_by,
        having,
        order_by,
    }))
}

/// Parse SELECT column list
fn parse_select_columns(parser: &mut Parser) -> ParseResult<Vec<SelectColumn>> {
    let mut columns = Vec::new();
    
    loop {
        match &parser.current_token {
            Some(token) => {
                let token_type = token.token_type.clone();
                match token_type {
                    TokenType::ASTERISK => {
                        columns.push(SelectColumn::Wildcard);
                        parser.next_token();
                    }
                    TokenType::COUNT | TokenType::SUM | TokenType::AVG | 
                    TokenType::MIN | TokenType::MAX => {
                        // Handle aggregate function
                        let expr = parse_expression(parser, 0)?;
                        
                        // Check for optional alias
                        let alias = parse_optional_alias(parser)?;
                        
                        columns.push(SelectColumn::Expression {
                            expr: Box::new(expr),
                            alias,
                        });
                    }
                    _ => {
                        // These are all tokens that could start an expression
                        let expr = parse_expression(parser, 0)?;
                        
                        // Check for optional alias
                        let alias = parse_optional_alias(parser)?;
                        
                        // If this is a simple column reference, use SelectColumn::Column
                        if let Expression::Column(col_ref) = expr {
                            if alias.is_none() {
                                columns.push(SelectColumn::Column(col_ref));
                            } else {
                                columns.push(SelectColumn::Expression {
                                    expr: Box::new(Expression::Column(col_ref)),
                                    alias,
                                });
                            }
                        } else {
                            columns.push(SelectColumn::Expression {
                                expr: Box::new(expr),
                                alias,
                            });
                        }
                    }
                }
            },
            None => return Err(ParseError::EndOfInput),
        }
        
        // Check if we have a comma for more columns
        if parser.current_token_is(TokenType::COMMA) {
            parser.next_token(); // Consume comma
            continue;
        }
        
        break;
    }
    
    Ok(columns)
}

/// Parse table references (FROM clause)
fn parse_table_references(parser: &mut Parser) -> ParseResult<Vec<TableReference>> {
    let mut tables = Vec::new();
    
    loop {
        let table = parse_table_reference(parser)?;
        tables.push(table);
        
        // Check if we have a comma for more tables
        if parser.current_token_is(TokenType::COMMA) {
            parser.next_token(); // Consume comma
            continue;
        }
        
        break;
    }
    
    Ok(tables)
}

/// Parse a single table reference
fn parse_table_reference(parser: &mut Parser) -> ParseResult<TableReference> {
    match &parser.current_token {
        Some(token) => {
            if let TokenType::IDENTIFIER(name) = &token.token_type {
                let table_name = name.clone();
                parser.next_token();
                
                // Check for optional alias
                let alias = if let Some(token) = &parser.current_token {
                    if let TokenType::IDENTIFIER(alias_name) = &token.token_type {
                        let name = alias_name.clone();
                        parser.next_token();
                        Some(name)
                    } else if token.token_type == TokenType::AS {
                        parser.next_token(); // Consume AS
                        
                        if let Some(tok) = &parser.current_token {
                            if let TokenType::IDENTIFIER(alias_name) = &tok.token_type {
                                let name = alias_name.clone();
                                parser.next_token();
                                Some(name)
                            } else {
                                return Err(ParseError::ExpectedToken(
                                    TokenType::IDENTIFIER("alias".to_string()),
                                    tok.clone()
                                ));
                            }
                        } else {
                            return Err(ParseError::EndOfInput);
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };
                
                Ok(TableReference {
                    name: table_name,
                    alias,
                })
            } else {
                Err(ParseError::UnexpectedToken(token.clone()))
            }
        },
        None => Err(ParseError::EndOfInput),
    }
}

/// Parse a JOIN clause
fn parse_join_clause(parser: &mut Parser) -> ParseResult<JoinClause> {
    // Parse join type
    let join_type = parse_join_type(parser)?;
    
    // Parse the table being joined
    let table = parse_table_reference(parser)?;
    
    let condition = if parser.current_token_is(TokenType::ON) {
        parser.expect_token(TokenType::ON)?; // Consume ON
        Some(parse_expression(parser, 0)?) // Parse join condition
    } else {
        // For join types like CROSS JOIN, ON is not present. 
        // Other join types might make it optional or mandatory based on SQL dialect.
        // For now, if ON is not found, condition is None.
        // A stricter parser might error here for INNER/LEFT/RIGHT/FULL JOINs if ON is missing.
        None 
    };
    
    Ok(JoinClause {
        join_type,
        table,
        condition, // Changed from Box::new to Option
    })
}

/// Parse the type of JOIN
fn parse_join_type(parser: &mut Parser) -> ParseResult<JoinType> {
    // Default join type is INNER
    let mut join_type = JoinType::Inner;
    
    // Check for different join types
    match &parser.current_token {
        Some(token) => {
            match &token.token_type {
                TokenType::JOIN => {
                    // Just plain JOIN means INNER JOIN
                    parser.next_token();
                },
                TokenType::INNER => {
                    parser.next_token(); // Consume INNER
                    parser.expect_token(TokenType::JOIN)?; // Expect JOIN
                },
                TokenType::LEFT => {
                    parser.next_token(); // Consume LEFT
                    
                    // OUTER is optional in LEFT OUTER JOIN
                    if parser.current_token_is(TokenType::OUTER) {
                        parser.next_token(); // Consume OUTER
                    }
                    
                    parser.expect_token(TokenType::JOIN)?; // Expect JOIN
                    join_type = JoinType::LeftOuter;
                },
                TokenType::RIGHT => {
                    parser.next_token(); // Consume RIGHT
                    
                    // OUTER is optional in RIGHT OUTER JOIN
                    if parser.current_token_is(TokenType::OUTER) {
                        parser.next_token(); // Consume OUTER
                    }
                    
                    parser.expect_token(TokenType::JOIN)?; // Expect JOIN
                    join_type = JoinType::RightOuter;
                },
                TokenType::FULL => {
                    parser.next_token(); // Consume FULL
                    
                    // OUTER is optional in FULL OUTER JOIN
                    if parser.current_token_is(TokenType::OUTER) {
                        parser.next_token(); // Consume OUTER
                    }
                    
                    parser.expect_token(TokenType::JOIN)?; // Expect JOIN
                    join_type = JoinType::FullOuter;
                },
                TokenType::CROSS => {
                    parser.next_token(); // Consume CROSS
                    parser.expect_token(TokenType::JOIN)?; // Expect JOIN
                    join_type = JoinType::Cross;
                },
                _ => {
                    return Err(ParseError::UnexpectedToken(token.clone()));
                }
            }
        },
        None => return Err(ParseError::EndOfInput),
    }
    
    Ok(join_type)
}

/// Parse expressions in the GROUP BY clause
fn parse_group_by_expressions(parser: &mut Parser) -> ParseResult<Vec<Expression>> {
    let mut expressions = Vec::new();
    
    loop {
        let expr = parse_expression(parser, 0)?;
        expressions.push(expr);
        
        // Check if we have a comma for more expressions
        if parser.current_token_is(TokenType::COMMA) {
            parser.next_token(); // Consume comma
            continue;
        }
        
        break;
    }
    
    Ok(expressions)
}

/// Parse an optional column alias (after AS keyword)
fn parse_optional_alias(parser: &mut Parser) -> ParseResult<Option<String>> {
    // First check for AS keyword
    if parser.current_token_is(TokenType::AS) {
        parser.next_token(); // Consume AS
        
        if let Some(token) = &parser.current_token {
            if let TokenType::IDENTIFIER(name) = &token.token_type {
                let alias = name.clone();
                parser.next_token();
                return Ok(Some(alias));
            } else {
                return Err(ParseError::ExpectedToken(
                    TokenType::IDENTIFIER("alias".to_string()),
                    token.clone()
                ));
            }
        } else {
            return Err(ParseError::EndOfInput);
        }
    }
    
    // We don't have 'AS', check for an implicit alias (identifier)
    if let Some(token) = &parser.current_token {
        // Check for identifier that might be an implicit alias
        // (without AS keyword)
        if let TokenType::IDENTIFIER(name) = &token.token_type {
            // Only treat as alias if it's not a keyword
            let is_keyword = name.eq_ignore_ascii_case("FROM") || 
                             name.eq_ignore_ascii_case("WHERE") ||
                             name.eq_ignore_ascii_case("GROUP") ||
                             name.eq_ignore_ascii_case("ORDER") ||
                             name.eq_ignore_ascii_case("HAVING") ||
                             name.eq_ignore_ascii_case("LIMIT");
            
            if !is_keyword {
                let alias = name.clone();
                parser.next_token(); // Consume the identifier
                return Ok(Some(alias));
            }
        }
    }
    
    // No alias found
    Ok(None)
}

/// Parse an ORDER BY clause
fn parse_order_by_clause(parser: &mut Parser) -> ParseResult<Vec<(Expression, bool)>> {
    parser.expect_token(TokenType::ORDER)?;
    parser.expect_token(TokenType::BY)?;

    let mut order_expressions = Vec::new();

    loop {
        let expr = parse_expression(parser, 0)?;
        let mut is_desc = false;

        if parser.current_token_is(TokenType::ASC) {
            parser.next_token(); // Consume ASC
        } else if parser.current_token_is(TokenType::DESC) {
            parser.next_token(); // Consume DESC
            is_desc = true;
        }

        order_expressions.push((expr, is_desc));

        if parser.current_token_is(TokenType::COMMA) {
            parser.next_token(); // Consume comma
        } else {
            break; // No more expressions in ORDER BY
        }
    }

    Ok(order_expressions)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_simple_select() {
        let mut parser = Parser::new("SELECT id, name FROM users");
        let result = parse_select(&mut parser);
        assert!(result.is_ok());
        
        if let Ok(Statement::Select(select)) = result {
            assert_eq!(select.columns.len(), 2);
            assert_eq!(select.from.len(), 1);
            assert!(select.where_clause.is_none());
            
            // Check table reference
            let table = &select.from[0];
            assert_eq!(table.name, "users");
            assert!(table.alias.is_none());
        } else {
            panic!("Expected SELECT statement");
        }
    }
    
    #[test]
    fn test_parse_select_with_where() {
        let mut parser = Parser::new("SELECT id, name FROM users WHERE id = 1");
        let result = parse_select(&mut parser);
        assert!(result.is_ok());
        
        if let Ok(Statement::Select(select)) = result {
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
        let mut parser = Parser::new("SELECT u.id, u.name, o.order_id FROM users u JOIN orders o ON u.id = o.user_id");
        let result = parse_select(&mut parser);
        assert!(result.is_ok());
        
        if let Ok(Statement::Select(select)) = result {
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
    fn test_parse_group_by_having() {
        let mut parser = Parser::new("SELECT department_id, COUNT(*) FROM employees GROUP BY department_id HAVING COUNT(*) > 5");
        let result = parse_select(&mut parser);
        assert!(result.is_ok());
        
        if let Ok(Statement::Select(select)) = result {
            // Check GROUP BY
            assert!(select.group_by.is_some());
            assert_eq!(select.group_by.unwrap().len(), 1);
            
            // Check HAVING
            assert!(select.having.is_some());
            if let Some(expr) = select.having {
                if let Expression::BinaryOp { op, .. } = *expr {
                    assert_eq!(op, Operator::GreaterThan);
                } else {
                    panic!("Expected binary operation in HAVING clause");
                }
            }
        } else {
            panic!("Expected SELECT statement");
        }
    }
    
    #[test]
    fn test_parse_aliases() {
        let mut parser = Parser::new("SELECT id AS user_id, name employee_name FROM users u");
        let result = parse_select(&mut parser);
        assert!(result.is_ok());
        
        if let Ok(Statement::Select(select)) = result {
            assert_eq!(select.columns.len(), 2);
            
            // Check column aliases
            if let SelectColumn::Expression { alias, .. } = &select.columns[0] {
                assert_eq!(alias, &Some("user_id".to_string()));
            } else {
                panic!("Expected aliased column");
            }
            
            if let SelectColumn::Expression { alias, .. } = &select.columns[1] {
                assert_eq!(alias, &Some("employee_name".to_string()));
            } else {
                panic!("Expected aliased column");
            }
            
            // Check table alias
            assert_eq!(select.from[0].alias, Some("u".to_string()));
        } else {
            panic!("Expected SELECT statement");
        }
    }
} 