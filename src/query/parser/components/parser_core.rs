// Core Parser Implementation
//
// This module implements the core parser functionality for SQL

use std::iter::Peekable;
use std::vec::IntoIter;
use std::fmt;

use crate::query::parser::ast::*;
use crate::query::parser::lexer::{Lexer, Token, TokenType};

/// SQL Parsing errors
#[derive(Debug, Clone)]
pub enum ParseError {
    UnexpectedToken(Token),
    ExpectedToken(TokenType, Token),
    InvalidOperator(String),
    InvalidLiteral(String),
    EndOfInput,
    NotYetImplemented(String),
    InvalidSyntax(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::UnexpectedToken(token) => write!(f, "Unexpected token: {:?}", token),
            ParseError::ExpectedToken(expected, actual) => write!(f, "Expected token: {:?}, found: {:?}", expected, actual),
            ParseError::InvalidOperator(op) => write!(f, "Invalid operator: {}", op),
            ParseError::InvalidLiteral(lit) => write!(f, "Invalid literal: {}", lit),
            ParseError::EndOfInput => write!(f, "Unexpected end of input"),
            ParseError::NotYetImplemented(feature) => write!(f, "Not yet implemented: {}", feature),
            ParseError::InvalidSyntax(reason) => write!(f, "Invalid syntax: {}", reason),
        }
    }
}

impl std::error::Error for ParseError {}

/// Result type for parsing operations
pub type ParseResult<T> = Result<T, ParseError>;

/// SQL Parser for constructing an AST from SQL tokens
pub struct Parser {
    pub tokens: Peekable<IntoIter<Token>>,
    pub current_token: Option<Token>,
    pub line: Option<usize>,
    pub column: Option<usize>,
}

impl Parser {
    /// Create a new parser from a SQL query string
    pub fn new(input: &str) -> Self {
        let mut lexer = Lexer::new(input);
        let mut tokens = Vec::new();
        
        loop {
            let token = lexer.next_token();
            let is_eof = matches!(token.token_type, TokenType::EOF);
            tokens.push(token);
            if is_eof {
                break;
            }
        }
        
        let mut parser = Parser {
            tokens: tokens.into_iter().peekable(),
            current_token: None,
            line: None,
            column: None,
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
            line: None,
            column: None,
        };
        
        parser.next_token();
        parser
    }
    
    /// Advance to the next token
    pub fn next_token(&mut self) -> Option<Token> {
        self.current_token = self.tokens.next();
        self.current_token.clone()
    }
    
    /// Peek at the next token without consuming it
    pub fn peek_token(&mut self) -> Option<&Token> {
        self.tokens.peek()
    }
    
    /// Check if the current token matches the expected type
    pub fn expect_token(&mut self, expected: TokenType) -> ParseResult<Token> {
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
    pub fn current_token_is(&self, token_type: TokenType) -> bool {
        match &self.current_token {
            Some(token) => matches_token_type(&token.token_type, &token_type),
            None => false,
        }
    }
    
    /// Parse an identifier (common utility)
    pub fn parse_identifier(&mut self) -> ParseResult<String> {
        match self.current_token.clone() {
            Some(token) => {
                if let TokenType::IDENTIFIER(name) = &token.token_type {
                    let identifier = name.clone();
                    self.next_token();
                    Ok(identifier)
                } else {
                    Err(ParseError::ExpectedToken(TokenType::IDENTIFIER(String::new()), token))
                }
            },
            None => Err(ParseError::EndOfInput),
        }
    }

    /// Get the literal string of the current token, or an error if no token or no literal.
    pub fn current_token_literal_or_err(&self) -> ParseResult<String> {
        self.current_token.as_ref().map(|t| t.literal.clone())
            .ok_or_else(|| ParseError::UnexpectedToken(
                self.current_token.clone().unwrap_or_else(|| Token {
                    token_type: TokenType::ILLEGAL, // Dummy token for error
                    literal: "<no token>".to_string(),
                    line: self.line.unwrap_or(0), // Assuming Parser has line/col info
                    column: self.column.unwrap_or(0),
                })
            ))
    }

    /// Get current token type, if any
    pub fn current_token_type(&self) -> Option<TokenType> {
        self.current_token.as_ref().map(|t| t.token_type.clone())
    }

    /// Peek at the next token's type without consuming current
    pub fn peek_token_type(&mut self) -> Option<TokenType> {
        self.tokens.peek().map(|t| t.token_type.clone())
    }

    /// Check if the next token is of a specific type
    pub fn peek_token_is(&mut self, expected_type: TokenType) -> bool {
        self.tokens.peek().is_some_and(|t| matches_token_type(&t.token_type, &expected_type))
    }
}

/// Helper function to check if a token type matches the expected type
pub fn matches_token_type(token_type: &TokenType, expected: &TokenType) -> bool {
    match (token_type, expected) {
        // Handle IDENTIFIER separately
        (TokenType::IDENTIFIER(actual_val), TokenType::IDENTIFIER(expected_val)) => {
            if expected_val.is_empty() { // If expected is generic IDENTIFIER (e.g. TokenType::IDENTIFIER(String::new()))
                true
            } else { // If expected is specific IDENTIFIER (e.g. TokenType::IDENTIFIER("KEY".to_string()))
                actual_val.eq_ignore_ascii_case(expected_val) // Case-insensitive for keywords
            }
        }
        // For other token types, just check if they're the same variant (discriminant)
        // This is safe because other variants either don't have data or their data isn't used for this kind of matching.
        _ => std::mem::discriminant(token_type) == std::mem::discriminant(expected),
    }
}

/// Convert a token type to an operator
pub fn token_to_operator(token_type: &TokenType) -> ParseResult<Operator> {
    match token_type {
        TokenType::EQ | TokenType::ASSIGN => Ok(Operator::Equals),
        TokenType::NotEq => Ok(Operator::NotEquals),
        TokenType::LT => Ok(Operator::LessThan),
        TokenType::GT => Ok(Operator::GreaterThan),
        TokenType::LtEq => Ok(Operator::LessEquals),
        TokenType::GtEq => Ok(Operator::GreaterEquals),
        TokenType::PLUS => Ok(Operator::Plus),
        TokenType::MINUS => Ok(Operator::Minus),
        TokenType::ASTERISK => Ok(Operator::Multiply),
        TokenType::SLASH => Ok(Operator::Divide),
        TokenType::PERCENT => Ok(Operator::Modulo),
        // Add other operators like AND, OR if they are token types
        // TokenType::AND => Ok(Operator::And),
        // TokenType::OR => Ok(Operator::Or),
        // TokenType::BANG / TokenType::NOT (if lexer has specific NOT token for operator) => Ok(Operator::Not)
        _ => Err(ParseError::InvalidSyntax(format!("Not an operator: {:?}", token_type))),
    }
}

/// Get operator precedence for expression parsing
pub fn get_operator_precedence(token_type: &TokenType) -> u8 {
    match token_type {
        TokenType::EQ | TokenType::ASSIGN | TokenType::NotEq | TokenType::LT | TokenType::GT | TokenType::LtEq | TokenType::GtEq => 1,
        TokenType::PLUS | TokenType::MINUS => 2,
        TokenType::ASTERISK | TokenType::SLASH | TokenType::PERCENT => 3,
        // TokenType::AND => 4, // Example for logical operators
        // TokenType::OR => 5,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_current_token_is() {
        let mut parser = Parser::new("SELECT * FROM users");
        assert!(parser.current_token_is(TokenType::SELECT));
        
        parser.next_token(); // Move to *
        assert!(parser.current_token_is(TokenType::ASTERISK));
        
        parser.next_token(); // Move to FROM
        assert!(parser.current_token_is(TokenType::FROM));
    }
    
    #[test]
    fn test_expect_token() {
        let mut parser = Parser::new("SELECT * FROM users");
        
        // Expect SELECT
        let token = parser.expect_token(TokenType::SELECT);
        assert!(token.is_ok());
        
        // Expect * (ASTERISK)
        let token = parser.expect_token(TokenType::ASTERISK);
        assert!(token.is_ok());
        
        // Expect FROM
        let token = parser.expect_token(TokenType::FROM);
        assert!(token.is_ok());
        
        // Expect IDENTIFIER
        let token = parser.expect_token(TokenType::IDENTIFIER(String::new()));
        assert!(token.is_ok());
        
        // Expect EOF
        let token = parser.expect_token(TokenType::EOF);
        assert!(token.is_ok());
    }
    
    #[test]
    fn test_parse_identifier() {
        let mut parser = Parser::new("users WHERE id = 1");
        
        let ident = parser.parse_identifier();
        assert!(ident.is_ok());
        assert_eq!(ident.unwrap(), "users");
        
        // Expect failure on WHERE (not an identifier)
        let ident = parser.parse_identifier();
        assert!(ident.is_err());
    }

    #[test]
    fn test_matches_token_type_helper() {
        assert!(matches_token_type(&TokenType::SELECT, &TokenType::SELECT));
        assert!(!matches_token_type(&TokenType::SELECT, &TokenType::FROM));
        assert!(matches_token_type(&TokenType::IDENTIFIER("abc".to_string()), &TokenType::IDENTIFIER(String::new())));
        assert!(matches_token_type(&TokenType::INTEGER, &TokenType::INTEGER));
        assert!(!matches_token_type(&TokenType::INTEGER, &TokenType::FLOAT));
    }

    #[test]
    fn test_token_to_operator_helper() {
        assert_eq!(token_to_operator(&TokenType::EQ).unwrap(), Operator::Equals);
        assert_eq!(token_to_operator(&TokenType::NotEq).unwrap(), Operator::NotEquals);
        assert_eq!(token_to_operator(&TokenType::LT).unwrap(), Operator::LessThan);
        assert_eq!(token_to_operator(&TokenType::GT).unwrap(), Operator::GreaterThan);
        assert_eq!(token_to_operator(&TokenType::LtEq).unwrap(), Operator::LessEquals);
        assert_eq!(token_to_operator(&TokenType::GtEq).unwrap(), Operator::GreaterEquals);
        assert_eq!(token_to_operator(&TokenType::PLUS).unwrap(), Operator::Plus);
        assert_eq!(token_to_operator(&TokenType::MINUS).unwrap(), Operator::Minus);
        assert_eq!(token_to_operator(&TokenType::ASTERISK).unwrap(), Operator::Multiply);
        assert_eq!(token_to_operator(&TokenType::SLASH).unwrap(), Operator::Divide);
        assert_eq!(token_to_operator(&TokenType::PERCENT).unwrap(), Operator::Modulo);
        
        assert!(token_to_operator(&TokenType::SELECT).is_err());
        assert!(token_to_operator(&TokenType::IDENTIFIER("id".to_string())).is_err());
    }

    #[test]
    fn test_get_operator_precedence_helper() {
        assert_eq!(get_operator_precedence(&TokenType::EQ), 1);
        assert_eq!(get_operator_precedence(&TokenType::PLUS), 2);
        assert_eq!(get_operator_precedence(&TokenType::ASTERISK), 3);
        assert_eq!(get_operator_precedence(&TokenType::SELECT), 0);
        assert_eq!(get_operator_precedence(&TokenType::LPAREN), 0);
    }
} 