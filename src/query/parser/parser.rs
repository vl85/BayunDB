// SQL Parser Implementation
//
// This module implements a recursive descent parser for SQL that converts
// tokens from the lexer into an Abstract Syntax Tree (AST).

use std::iter::Peekable;
use std::vec::IntoIter;

use super::ast::*;
use super::lexer::{Lexer, Token, TokenType};

/// SQL Parsing errors
#[derive(Debug)]
pub enum ParseError {
    UnexpectedToken(Token),
    ExpectedToken(TokenType, Token),
    InvalidSyntax(String),
    EndOfInput,
}

/// Result type for parsing operations
pub type ParseResult<T> = Result<T, ParseError>;

/// SQL Parser for constructing an AST from SQL tokens
pub struct Parser {
    tokens: Peekable<IntoIter<Token>>,
    current_token: Option<Token>,
}

impl Parser {
    /// Create a new parser from a SQL query string
    pub fn new(input: &str) -> Self {
        let mut lexer = Lexer::new(input);
        let mut tokens = Vec::new();
        
        loop {
            let token = lexer.next_token();
            let is_eof = token.token_type == TokenType::EOF;
            tokens.push(token);
            if is_eof {
                break;
            }
        }
        
        let mut parser = Parser {
            tokens: tokens.into_iter().peekable(),
            current_token: None,
        };
        
        parser.next_token();
        parser
    }
    
    /// Create a parser from a vector of tokens (for testing)
    #[cfg(test)]
    pub fn from_tokens(tokens: Vec<Token>) -> Self {
        let mut parser = Parser {
            tokens: tokens.into_iter().peekable(),
            current_token: None,
        };
        
        parser.next_token();
        parser
    }
    
    /// Advance to the next token
    fn next_token(&mut self) -> Option<Token> {
        self.current_token = self.tokens.next();
        self.current_token.clone()
    }
    
    /// Peek at the next token without consuming it
    fn peek_token(&mut self) -> Option<&Token> {
        self.tokens.peek()
    }
    
    /// Check if the current token matches the expected type
    fn expect_token(&mut self, expected: TokenType) -> ParseResult<Token> {
        match self.current_token.clone() {
            Some(token) if matches_token_type(&token.token_type, &expected) => {
                let current = token;
                self.next_token();
                Ok(current)
            }
            Some(token) => Err(ParseError::ExpectedToken(expected, token)),
            None => Err(ParseError::EndOfInput),
        }
    }
    
    /// Check if the current token is of the given type
    fn current_token_is(&self, token_type: TokenType) -> bool {
        match &self.current_token {
            Some(token) => matches_token_type(&token.token_type, &token_type),
            None => false,
        }
    }
    
    /// Parse a SQL statement
    pub fn parse_statement(&mut self) -> ParseResult<Statement> {
        match &self.current_token {
            Some(token) => {
                let token_type = token.token_type.clone();
                match token_type {
                    TokenType::SELECT => self.parse_select(),
                    TokenType::INSERT => self.parse_insert(),
                    TokenType::UPDATE => self.parse_update(),
                    TokenType::DELETE => self.parse_delete(),
                    TokenType::CREATE => self.parse_create(),
                    _ => Err(ParseError::UnexpectedToken(token.clone())),
                }
            },
            None => Err(ParseError::EndOfInput),
        }
    }
    
    /// Parse a SELECT statement
    fn parse_select(&mut self) -> ParseResult<Statement> {
        // Consume SELECT keyword
        self.expect_token(TokenType::SELECT)?;
        
        // Parse columns
        let columns = self.parse_select_columns()?;
        
        // Expect FROM keyword
        self.expect_token(TokenType::FROM)?;
        
        // Parse FROM clause
        let from = self.parse_table_references()?;
        
        // Parse optional JOIN clauses
        let mut joins = Vec::new();
        while self.current_token_is(TokenType::JOIN) || 
              self.current_token_is(TokenType::INNER) ||
              self.current_token_is(TokenType::LEFT) ||
              self.current_token_is(TokenType::RIGHT) ||
              self.current_token_is(TokenType::FULL) ||
              self.current_token_is(TokenType::CROSS) {
            joins.push(self.parse_join_clause()?);
        }
        
        // Parse optional WHERE clause
        let where_clause = if self.current_token_is(TokenType::WHERE) {
            self.next_token(); // Consume WHERE
            Some(Box::new(self.parse_expression(0)?))
        } else {
            None
        };
        
        // Optional semicolon at the end
        if self.current_token_is(TokenType::SEMICOLON) {
            self.next_token();
        }
        
        Ok(Statement::Select(SelectStatement {
            columns,
            from,
            where_clause,
            joins,
        }))
    }
    
    /// Parse SELECT column list
    fn parse_select_columns(&mut self) -> ParseResult<Vec<SelectColumn>> {
        let mut columns = Vec::new();
        
        loop {
            match &self.current_token {
                Some(token) => {
                    let token_type = token.token_type.clone();
                    match token_type {
                        TokenType::MULTIPLY => {
                            columns.push(SelectColumn::Wildcard);
                            self.next_token();
                        }
                        TokenType::IDENTIFIER(_) => {
                            // Parse column reference
                            let col_ref = self.parse_column_reference()?;
                            columns.push(SelectColumn::Column(col_ref));
                        }
                        _ => {
                            return Err(ParseError::UnexpectedToken(token.clone()));
                        }
                    }
                },
                None => return Err(ParseError::EndOfInput),
            }
            
            // Check if we have a comma for more columns
            if self.current_token_is(TokenType::COMMA) {
                self.next_token(); // Consume comma
                continue;
            }
            
            break;
        }
        
        Ok(columns)
    }
    
    /// Parse table references (FROM clause)
    fn parse_table_references(&mut self) -> ParseResult<Vec<TableReference>> {
        let mut tables = Vec::new();
        
        loop {
            match &self.current_token {
                Some(token) => {
                    match &token.token_type {
                        TokenType::IDENTIFIER(name) => {
                            let table_name = name.clone();
                            self.next_token();
                            
                            // Check for optional alias
                            let alias = match &self.current_token {
                                Some(token) => {
                                    if let TokenType::IDENTIFIER(alias_name) = &token.token_type {
                                        let name = alias_name.clone();
                                        self.next_token();
                                        Some(name)
                                    } else {
                                        None
                                    }
                                },
                                None => None,
                            };
                            
                            tables.push(TableReference {
                                name: table_name,
                                alias,
                            });
                        }
                        _ => {
                            return Err(ParseError::UnexpectedToken(token.clone()));
                        }
                    }
                },
                None => return Err(ParseError::EndOfInput),
            }
            
            // Check if we have a comma for more tables
            if self.current_token_is(TokenType::COMMA) {
                self.next_token(); // Consume comma
                continue;
            }
            
            break;
        }
        
        Ok(tables)
    }
    
    /// Parse a column reference (possibly qualified with table name)
    fn parse_column_reference(&mut self) -> ParseResult<ColumnReference> {
        match &self.current_token {
            Some(token) => {
                if let TokenType::IDENTIFIER(name) = &token.token_type {
                    let name = name.clone();
                    self.next_token();
                    
                    // Check if we have a dot for a qualified column
                    if self.current_token_is(TokenType::DOT) {
                        self.next_token(); // Consume dot
                        
                        match &self.current_token {
                            Some(token) => {
                                if let TokenType::IDENTIFIER(col_name) = &token.token_type {
                                    let col_name = col_name.clone();
                                    self.next_token();
                                    Ok(ColumnReference {
                                        table: Some(name),
                                        name: col_name,
                                    })
                                } else {
                                    Err(ParseError::UnexpectedToken(token.clone()))
                                }
                            },
                            None => Err(ParseError::EndOfInput),
                        }
                    } else {
                        Ok(ColumnReference {
                            table: None,
                            name,
                        })
                    }
                } else {
                    Err(ParseError::UnexpectedToken(token.clone()))
                }
            },
            None => Err(ParseError::EndOfInput),
        }
    }
    
    /// Parse an expression with operator precedence
    fn parse_expression(&mut self, precedence: u8) -> ParseResult<Expression> {
        // First parse a prefix expression (literal, identifier, etc.)
        let mut left_expr = self.parse_prefix_expression()?;
        
        // Then handle infix expressions based on precedence
        loop {
            match &self.current_token {
                Some(token) => {
                    let current_precedence = get_operator_precedence(&token.token_type);
                    if precedence >= current_precedence {
                        break;
                    }
                    
                    left_expr = self.parse_infix_expression(left_expr)?;
                },
                None => break,
            }
        }
        
        Ok(left_expr)
    }
    
    /// Parse a prefix expression (literal, column reference, etc.)
    fn parse_prefix_expression(&mut self) -> ParseResult<Expression> {
        match &self.current_token {
            Some(token) => {
                match &token.token_type {
                    TokenType::INTEGER(value) => {
                        let val = *value;
                        self.next_token();
                        Ok(Expression::Literal(Value::Integer(val)))
                    }
                    TokenType::FLOAT(value) => {
                        let val = *value;
                        self.next_token();
                        Ok(Expression::Literal(Value::Float(val)))
                    }
                    TokenType::STRING(value) => {
                        let val = value.clone();
                        self.next_token();
                        Ok(Expression::Literal(Value::String(val)))
                    }
                    TokenType::IDENTIFIER(_) => {
                        let col_ref = self.parse_column_reference()?;
                        Ok(Expression::Column(col_ref))
                    }
                    TokenType::LeftParen => {
                        self.next_token(); // Consume left paren
                        let expr = self.parse_expression(0)?;
                        self.expect_token(TokenType::RightParen)?;
                        Ok(expr)
                    }
                    _ => Err(ParseError::UnexpectedToken(token.clone())),
                }
            },
            None => Err(ParseError::EndOfInput),
        }
    }
    
    /// Parse an infix expression (binary operations)
    fn parse_infix_expression(&mut self, left: Expression) -> ParseResult<Expression> {
        match &self.current_token {
            Some(token) => {
                let token_type = token.token_type.clone();
                let precedence = get_operator_precedence(&token_type);
                let op = token_to_operator(&token_type)?;
                self.next_token();
                
                let right = self.parse_expression(precedence)?;
                
                Ok(Expression::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                })
            }
            None => Err(ParseError::EndOfInput),
        }
    }
    
    /// Parse a JOIN clause
    fn parse_join_clause(&mut self) -> ParseResult<JoinClause> {
        // Parse join type
        let join_type = self.parse_join_type()?;
        
        // Parse the table being joined
        let table = self.parse_table_reference()?;
        
        // Expect ON keyword
        self.expect_token(TokenType::ON)?;
        
        // Parse join condition
        let condition = Box::new(self.parse_expression(0)?);
        
        Ok(JoinClause {
            join_type,
            table,
            condition,
        })
    }
    
    /// Parse the type of JOIN
    fn parse_join_type(&mut self) -> ParseResult<JoinType> {
        // Default join type is INNER
        let mut join_type = JoinType::Inner;
        
        // Check for different join types
        match &self.current_token {
            Some(token) => {
                match &token.token_type {
                    TokenType::JOIN => {
                        // Just plain JOIN means INNER JOIN
                        self.next_token();
                    },
                    TokenType::INNER => {
                        self.next_token(); // Consume INNER
                        self.expect_token(TokenType::JOIN)?; // Expect JOIN
                    },
                    TokenType::LEFT => {
                        self.next_token(); // Consume LEFT
                        
                        // OUTER is optional in LEFT OUTER JOIN
                        if self.current_token_is(TokenType::OUTER) {
                            self.next_token(); // Consume OUTER
                        }
                        
                        self.expect_token(TokenType::JOIN)?; // Expect JOIN
                        join_type = JoinType::LeftOuter;
                    },
                    TokenType::RIGHT => {
                        self.next_token(); // Consume RIGHT
                        
                        // OUTER is optional in RIGHT OUTER JOIN
                        if self.current_token_is(TokenType::OUTER) {
                            self.next_token(); // Consume OUTER
                        }
                        
                        self.expect_token(TokenType::JOIN)?; // Expect JOIN
                        join_type = JoinType::RightOuter;
                    },
                    TokenType::FULL => {
                        self.next_token(); // Consume FULL
                        
                        // OUTER is optional in FULL OUTER JOIN
                        if self.current_token_is(TokenType::OUTER) {
                            self.next_token(); // Consume OUTER
                        }
                        
                        self.expect_token(TokenType::JOIN)?; // Expect JOIN
                        join_type = JoinType::FullOuter;
                    },
                    TokenType::CROSS => {
                        self.next_token(); // Consume CROSS
                        self.expect_token(TokenType::JOIN)?; // Expect JOIN
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
    
    /// Parse a single table reference
    fn parse_table_reference(&mut self) -> ParseResult<TableReference> {
        match &self.current_token {
            Some(token) => {
                if let TokenType::IDENTIFIER(name) = &token.token_type {
                    let table_name = name.clone();
                    self.next_token();
                    
                    // Check for optional alias
                    let alias = match &self.current_token {
                        Some(token) => {
                            if let TokenType::IDENTIFIER(alias_name) = &token.token_type {
                                let name = alias_name.clone();
                                self.next_token();
                                Some(name)
                            } else {
                                None
                            }
                        },
                        None => None,
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
    
    // Placeholder implementations for other statement types
    fn parse_insert(&mut self) -> ParseResult<Statement> {
        Err(ParseError::InvalidSyntax("INSERT not implemented yet".to_string()))
    }
    
    fn parse_update(&mut self) -> ParseResult<Statement> {
        Err(ParseError::InvalidSyntax("UPDATE not implemented yet".to_string()))
    }
    
    fn parse_delete(&mut self) -> ParseResult<Statement> {
        Err(ParseError::InvalidSyntax("DELETE not implemented yet".to_string()))
    }
    
    fn parse_create(&mut self) -> ParseResult<Statement> {
        Err(ParseError::InvalidSyntax("CREATE not implemented yet".to_string()))
    }
}

/// Check if token matches expected type (handling identifiers separately)
fn matches_token_type(token_type: &TokenType, expected: &TokenType) -> bool {
    match (token_type, expected) {
        (TokenType::IDENTIFIER(_), TokenType::IDENTIFIER(_)) => true,
        _ => std::mem::discriminant(token_type) == std::mem::discriminant(expected),
    }
}

/// Convert a token to an operator
fn token_to_operator(token_type: &TokenType) -> ParseResult<Operator> {
    match token_type {
        TokenType::EQUALS => Ok(Operator::Equals),
        TokenType::NotEqual => Ok(Operator::NotEquals),
        TokenType::LessThan => Ok(Operator::LessThan),
        TokenType::GreaterThan => Ok(Operator::GreaterThan),
        TokenType::LessEqual => Ok(Operator::LessEquals),
        TokenType::GreaterEqual => Ok(Operator::GreaterEquals),
        TokenType::PLUS => Ok(Operator::Plus),
        TokenType::MINUS => Ok(Operator::Minus),
        TokenType::MULTIPLY => Ok(Operator::Multiply),
        TokenType::DIVIDE => Ok(Operator::Divide),
        _ => Err(ParseError::InvalidSyntax(format!("Not an operator: {:?}", token_type))),
    }
}

/// Get operator precedence
fn get_operator_precedence(token_type: &TokenType) -> u8 {
    match token_type {
        TokenType::EQUALS | TokenType::NotEqual | TokenType::LessThan 
        | TokenType::GreaterThan | TokenType::LessEqual | TokenType::GreaterEqual => 1,
        TokenType::PLUS | TokenType::MINUS => 2,
        TokenType::MULTIPLY | TokenType::DIVIDE => 3,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_simple_select() {
        let query = "SELECT id, name FROM users";
        let mut parser = Parser::new(query);
        
        let statement = parser.parse_statement().unwrap();
        
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
        let mut parser = Parser::new(query);
        
        let statement = parser.parse_statement().unwrap();
        
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
        let mut parser = Parser::new(query);
        
        let statement = parser.parse_statement().unwrap();
        
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
        let mut parser = Parser::new(query);
        
        let statement = parser.parse_statement().unwrap();
        
        if let Statement::Select(select) = statement {
            assert_eq!(select.joins.len(), 1);
            
            let join = &select.joins[0];
            assert_eq!(join.join_type, JoinType::LeftOuter);
            assert_eq!(join.table.name, "orders");
        } else {
            panic!("Expected SELECT statement");
        }
    }
} 