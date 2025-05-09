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
        
        // Parse optional GROUP BY clause
        let group_by = if self.current_token_is(TokenType::GROUP) {
            self.next_token(); // Consume GROUP
            self.expect_token(TokenType::BY)?; // Expect BY
            Some(self.parse_group_by_expressions()?)
        } else {
            None
        };
        
        // Parse optional HAVING clause
        let having = if self.current_token_is(TokenType::HAVING) {
            self.next_token(); // Consume HAVING
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
            group_by,
            having,
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
                        TokenType::COUNT | TokenType::SUM | TokenType::AVG | 
                        TokenType::MIN | TokenType::MAX => {
                            // Handle aggregate function
                            let expr = self.parse_aggregate_function(token_type)?;
                            
                            // Check for optional alias
                            let alias = self.parse_optional_alias()?;
                            
                            columns.push(SelectColumn::Expression {
                                expr: Box::new(expr),
                                alias,
                            });
                        }
                        TokenType::IDENTIFIER(_) | TokenType::INTEGER(_) | TokenType::FLOAT(_) |
                        TokenType::STRING(_) | TokenType::LeftParen => {
                            // These are all tokens that could start an expression
                            let expr = self.parse_expression(0)?;
                            
                            // Check for optional alias
                            let alias = self.parse_optional_alias()?;
                            
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
    
    /// Parse a prefix expression (literal, identifier, etc.)
    fn parse_prefix_expression(&mut self) -> ParseResult<Expression> {
        match &self.current_token {
            Some(token) => {
                let token_type = token.token_type.clone();
                match token_type {
                    TokenType::INTEGER(val) => {
                        self.next_token();
                        Ok(Expression::Literal(Value::Integer(val)))
                    },
                    TokenType::FLOAT(val) => {
                        self.next_token();
                        Ok(Expression::Literal(Value::Float(val)))
                    },
                    TokenType::STRING(val) => {
                        self.next_token();
                        Ok(Expression::Literal(Value::String(val)))
                    },
                    TokenType::IDENTIFIER(_) => {
                        // Parse column reference
                        let col_ref = self.parse_column_reference()?;
                        Ok(Expression::Column(col_ref))
                    },
                    TokenType::COUNT | TokenType::SUM | TokenType::AVG | 
                    TokenType::MIN | TokenType::MAX => {
                        self.parse_aggregate_function(token_type)
                    },
                    TokenType::LeftParen => {
                        self.next_token(); // Consume left paren
                        let expr = self.parse_expression(0)?;
                        self.expect_token(TokenType::RightParen)?; // Expect right paren
                        Ok(expr)
                    },
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
    
    /// Parse expressions in the GROUP BY clause
    fn parse_group_by_expressions(&mut self) -> ParseResult<Vec<Expression>> {
        let mut expressions = Vec::new();
        
        loop {
            let expr = self.parse_expression(0)?;
            expressions.push(expr);
            
            // Check if we have a comma for more expressions
            if self.current_token_is(TokenType::COMMA) {
                self.next_token(); // Consume comma
                continue;
            }
            
            break;
        }
        
        Ok(expressions)
    }
    
    /// Parse an aggregate function expression
    fn parse_aggregate_function(&mut self, token_type: TokenType) -> ParseResult<Expression> {
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
        self.next_token();
        
        // Expect opening parenthesis
        self.expect_token(TokenType::LeftParen)?;
        
        // Parse the argument (could be * for COUNT(*))
        let arg = if self.current_token_is(TokenType::MULTIPLY) {
            self.next_token(); // Consume *
            None
        } else {
            Some(Box::new(self.parse_expression(0)?))
        };
        
        // Expect closing parenthesis
        self.expect_token(TokenType::RightParen)?;
        
        Ok(Expression::Aggregate { function, arg })
    }
    
    /// Parse an optional column alias (after AS keyword)
    fn parse_optional_alias(&mut self) -> ParseResult<Option<String>> {
        // First check for AS keyword
        if self.current_token_is(TokenType::IDENTIFIER("AS".to_string())) || 
           self.current_token_is(TokenType::AS) {
            self.next_token(); // Consume AS
            if let Some(token) = self.next_token() {
                if let TokenType::IDENTIFIER(name) = token.token_type {
                    Ok(Some(name))
                } else {
                    Err(ParseError::ExpectedToken(
                        TokenType::IDENTIFIER("alias".to_string()),
                        token
                    ))
                }
            } else {
                Err(ParseError::EndOfInput)
            }
        } else {
            // We don't have 'AS', check for an implicit alias (identifier)
            let current_token_opt = self.current_token.clone();
            
            if let Some(token) = current_token_opt {
                // Check for identifier that might be an implicit alias
                // (without AS keyword)
                if let TokenType::IDENTIFIER(name) = token.token_type {
                    // Only treat as alias if it's not a keyword
                    let is_keyword = name.eq_ignore_ascii_case("FROM") || 
                                     name.eq_ignore_ascii_case("WHERE") ||
                                     name.eq_ignore_ascii_case("GROUP") ||
                                     name.eq_ignore_ascii_case("ORDER") ||
                                     name.eq_ignore_ascii_case("HAVING") ||
                                     name.eq_ignore_ascii_case("LIMIT");
                    
                    if !is_keyword {
                        self.next_token(); // Consume the identifier
                        Ok(Some(name))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        }
    }
    
    /// Parse a CREATE statement
    fn parse_create(&mut self) -> ParseResult<Statement> {
        // Consume CREATE keyword
        self.expect_token(TokenType::CREATE)?;
        
        // Check what we're creating
        if self.current_token_is(TokenType::TABLE) {
            self.parse_create_table()
        } else {
            Err(ParseError::InvalidSyntax("Only CREATE TABLE is supported".to_string()))
        }
    }
    
    /// Parse a CREATE TABLE statement
    fn parse_create_table(&mut self) -> ParseResult<Statement> {
        // Consume TABLE keyword
        self.expect_token(TokenType::TABLE)?;
        
        // Parse table name
        let table_name = self.parse_identifier()?;
        
        // Expect opening parenthesis
        self.expect_token(TokenType::LeftParen)?;
        
        // Parse column definitions
        let columns = self.parse_column_definitions()?;
        
        // Expect closing parenthesis
        self.expect_token(TokenType::RightParen)?;
        
        // Optional semicolon
        if self.current_token_is(TokenType::SEMICOLON) {
            self.next_token();
        }
        
        Ok(Statement::Create(CreateStatement {
            table_name,
            columns,
        }))
    }
    
    /// Parse column definitions
    fn parse_column_definitions(&mut self) -> ParseResult<Vec<ColumnDef>> {
        let mut columns = Vec::new();
        
        loop {
            // Parse column name
            let name = self.parse_identifier()?;
            
            // Parse data type
            let data_type = self.parse_data_type()?;
            
            // Check for constraints
            let mut nullable = true;
            let mut primary_key = false;
            
            // Look for NOT NULL and PRIMARY KEY constraints
            // Use match to check identifier values with case insensitivity
            loop {
                match &self.current_token {
                    Some(token) => {
                        match &token.token_type {
                            TokenType::IDENTIFIER(s) if s.to_uppercase() == "NOT" => {
                                self.next_token(); // Consume NOT
                                
                                // Expect NULL
                                match &self.current_token {
                                    Some(token) => {
                                        match &token.token_type {
                                            TokenType::IDENTIFIER(s) if s.to_uppercase() == "NULL" => {
                                                self.next_token(); // Consume NULL
                                                nullable = false;
                                            },
                                            _ => {
                                                return Err(ParseError::ExpectedToken(
                                                    TokenType::IDENTIFIER("NULL".to_string()),
                                                    token.clone()
                                                ));
                                            }
                                        }
                                    },
                                    None => return Err(ParseError::EndOfInput),
                                }
                            },
                            TokenType::IDENTIFIER(s) if s.to_uppercase() == "PRIMARY" => {
                                self.next_token(); // Consume PRIMARY
                                
                                // Expect KEY
                                match &self.current_token {
                                    Some(token) => {
                                        match &token.token_type {
                                            TokenType::IDENTIFIER(s) if s.to_uppercase() == "KEY" => {
                                                self.next_token(); // Consume KEY
                                                primary_key = true;
                                                nullable = false; // Primary key columns cannot be null
                                            },
                                            _ => {
                                                return Err(ParseError::ExpectedToken(
                                                    TokenType::IDENTIFIER("KEY".to_string()),
                                                    token.clone()
                                                ));
                                            }
                                        }
                                    },
                                    None => return Err(ParseError::EndOfInput),
                                }
                            },
                            _ => break, // Not a constraint, break out of the loop
                        }
                    },
                    None => break, // End of input, break out of the loop
                }
            }
            
            // Add the column definition
            columns.push(ColumnDef {
                name,
                data_type,
                nullable,
                primary_key,
            });
            
            // Check if there are more columns
            if self.current_token_is(TokenType::COMMA) {
                self.next_token(); // Consume comma
                continue;
            }
            
            break;
        }
        
        Ok(columns)
    }
    
    /// Parse a data type
    fn parse_data_type(&mut self) -> ParseResult<DataType> {
        match &self.current_token {
            Some(token) => {
                match &token.token_type {
                    TokenType::IDENTIFIER(s) => {
                        // Check for common data types
                        let data_type = match s.to_uppercase().as_str() {
                            "INT" | "INTEGER" => DataType::Integer,
                            "FLOAT" | "REAL" | "DOUBLE" => DataType::Float,
                            "TEXT" | "VARCHAR" | "CHAR" | "STRING" => DataType::Text,
                            "BOOL" | "BOOLEAN" => DataType::Boolean,
                            "DATE" => DataType::Date,
                            "TIMESTAMP" | "DATETIME" => DataType::Timestamp,
                            _ => return Err(ParseError::InvalidSyntax(format!("Unknown data type: {}", s))),
                        };
                        
                        self.next_token(); // Consume the data type
                        
                        // Handle VARCHAR(n) or similar type specifications
                        if self.current_token_is(TokenType::LeftParen) {
                            self.next_token(); // Consume left paren
                            
                            // For now, we're ignoring the size parameter
                            if let Some(token) = &self.current_token {
                                if let TokenType::INTEGER(_) = token.token_type {
                                    self.next_token(); // Consume the size
                                }
                            }
                            
                            self.expect_token(TokenType::RightParen)?;
                        }
                        
                        Ok(data_type)
                    },
                    _ => Err(ParseError::UnexpectedToken(token.clone())),
                }
            },
            None => Err(ParseError::EndOfInput),
        }
    }
    
    /// Parse an identifier
    fn parse_identifier(&mut self) -> ParseResult<String> {
        match &self.current_token {
            Some(token) => {
                match &token.token_type {
                    TokenType::IDENTIFIER(s) => {
                        let identifier = s.clone();
                        self.next_token(); // Consume the identifier
                        Ok(identifier)
                    },
                    _ => Err(ParseError::UnexpectedToken(token.clone())),
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
        TokenType::MODULO => Ok(Operator::Modulo),
        _ => Err(ParseError::InvalidSyntax(format!("Not an operator: {:?}", token_type))),
    }
}

/// Get operator precedence
fn get_operator_precedence(token_type: &TokenType) -> u8 {
    match token_type {
        TokenType::EQUALS | TokenType::NotEqual | TokenType::LessThan 
        | TokenType::GreaterThan | TokenType::LessEqual | TokenType::GreaterEqual => 1,
        TokenType::PLUS | TokenType::MINUS => 2,
        TokenType::MULTIPLY | TokenType::DIVIDE | TokenType::MODULO => 3,
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
    
    #[test]
    fn test_parse_aggregate_functions() {
        // Test COUNT(*)
        let sql = "SELECT COUNT(*) FROM users";
        let mut parser = Parser::new(sql);
        
        let stmt = parser.parse_statement().unwrap();
        
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
        let mut parser = Parser::new(sql);
        
        let stmt = parser.parse_statement().unwrap();
        
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
        let mut parser = Parser::new(sql);
        
        let stmt = parser.parse_statement().unwrap();
        
        if let Statement::Select(select) = stmt {
            assert!(select.group_by.is_some());
            assert_eq!(select.group_by.unwrap().len(), 1);
            assert!(select.having.is_none());
        } else {
            panic!("Expected SELECT statement");
        }
        
        // Test with multiple GROUP BY columns
        let sql = "SELECT department_id, job_title, COUNT(*) FROM employees GROUP BY department_id, job_title";
        let mut parser = Parser::new(sql);
        
        let stmt = parser.parse_statement().unwrap();
        
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
        let mut parser = Parser::new(sql);
        
        let stmt = parser.parse_statement().unwrap();
        
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
        let mut parser = Parser::new(sql);
        
        let stmt = parser.parse_statement().unwrap();
        
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
        
        let mut parser = Parser::new(query);
        let stmt = parser.parse_statement().unwrap();
        
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
            
            // Check email column
            let email_col = &create_stmt.columns[2];
            assert_eq!(email_col.name, "email");
            assert_eq!(email_col.data_type, DataType::Text);
            assert_eq!(email_col.primary_key, false);
            assert_eq!(email_col.nullable, true);
            
            // Check created_at column
            let created_col = &create_stmt.columns[3];
            assert_eq!(created_col.name, "created_at");
            assert_eq!(created_col.data_type, DataType::Timestamp);
            assert_eq!(created_col.primary_key, false);
            assert_eq!(created_col.nullable, true);
        } else {
            panic!("Expected CREATE TABLE statement");
        }
    }
} 