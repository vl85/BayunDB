// Core Parser Implementation
//
// This module implements the core parser functionality for SQL

use std::iter::Peekable;
use std::vec::IntoIter;

use crate::query::parser::ast::*;
use crate::query::parser::lexer::{Lexer, Token, TokenType};

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
    pub tokens: Peekable<IntoIter<Token>>,
    pub current_token: Option<Token>,
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
}

/// Helper function to check if a token type matches the expected type
pub fn matches_token_type(token_type: &TokenType, expected: &TokenType) -> bool {
    match (token_type, expected) {
        // Handle IDENTIFIER separately since it contains a value
        (TokenType::IDENTIFIER(_), TokenType::IDENTIFIER(_)) => true,
        // For other token types, just check if they're the same variant
        _ => std::mem::discriminant(token_type) == std::mem::discriminant(expected),
    }
}

/// Convert a token type to an operator
pub fn token_to_operator(token_type: &TokenType) -> ParseResult<Operator> {
    match token_type {
        TokenType::EQUALS => Ok(Operator::Equals),
        TokenType::NotEqual => Ok(Operator::NotEquals),
        TokenType::LessThan => Ok(Operator::LessThan),
        TokenType::GreaterThan => Ok(Operator::GreaterThan),
        TokenType::LessEqual => Ok(Operator::LessEquals),
        TokenType::GreaterEqual => Ok(Operator::GreaterEquals),
        // Add other operators as needed
        TokenType::PLUS => Ok(Operator::Plus),
        TokenType::MINUS => Ok(Operator::Minus),
        TokenType::MULTIPLY => Ok(Operator::Multiply),
        TokenType::DIVIDE => Ok(Operator::Divide),
        TokenType::MODULO => Ok(Operator::Modulo),
        _ => Err(ParseError::InvalidSyntax(format!("Not an operator: {:?}", token_type))),
    }
}

/// Get operator precedence for expression parsing
pub fn get_operator_precedence(token_type: &TokenType) -> u8 {
    match token_type {
        TokenType::EQUALS | TokenType::NotEqual => 1, 
        TokenType::LessThan | TokenType::GreaterThan | 
        TokenType::LessEqual | TokenType::GreaterEqual => 1,
        TokenType::PLUS | TokenType::MINUS => 2,
        TokenType::MULTIPLY | TokenType::DIVIDE | TokenType::MODULO => 3,
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
        assert!(parser.current_token_is(TokenType::MULTIPLY));
        
        parser.next_token(); // Move to FROM
        assert!(parser.current_token_is(TokenType::FROM));
    }
    
    #[test]
    fn test_expect_token() {
        let mut parser = Parser::new("SELECT * FROM users");
        
        // Expect SELECT
        let token = parser.expect_token(TokenType::SELECT);
        assert!(token.is_ok());
        
        // Expect * (MULTIPLY)
        let token = parser.expect_token(TokenType::MULTIPLY);
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
        assert!(matches_token_type(&TokenType::INTEGER(123), &TokenType::INTEGER(0))); // Compares discriminant
        assert!(!matches_token_type(&TokenType::INTEGER(123), &TokenType::FLOAT(0.0)));
    }

    #[test]
    fn test_token_to_operator_helper() {
        assert_eq!(token_to_operator(&TokenType::EQUALS).unwrap(), Operator::Equals);
        assert_eq!(token_to_operator(&TokenType::NotEqual).unwrap(), Operator::NotEquals);
        assert_eq!(token_to_operator(&TokenType::LessThan).unwrap(), Operator::LessThan);
        assert_eq!(token_to_operator(&TokenType::GreaterThan).unwrap(), Operator::GreaterThan);
        assert_eq!(token_to_operator(&TokenType::LessEqual).unwrap(), Operator::LessEquals);
        assert_eq!(token_to_operator(&TokenType::GreaterEqual).unwrap(), Operator::GreaterEquals);
        assert_eq!(token_to_operator(&TokenType::PLUS).unwrap(), Operator::Plus);
        assert_eq!(token_to_operator(&TokenType::MINUS).unwrap(), Operator::Minus);
        assert_eq!(token_to_operator(&TokenType::MULTIPLY).unwrap(), Operator::Multiply);
        assert_eq!(token_to_operator(&TokenType::DIVIDE).unwrap(), Operator::Divide);
        assert_eq!(token_to_operator(&TokenType::MODULO).unwrap(), Operator::Modulo);
        
        assert!(token_to_operator(&TokenType::SELECT).is_err());
        assert!(token_to_operator(&TokenType::IDENTIFIER("id".to_string())).is_err());
    }

    #[test]
    fn test_get_operator_precedence_helper() {
        assert_eq!(get_operator_precedence(&TokenType::EQUALS), 1);
        assert_eq!(get_operator_precedence(&TokenType::PLUS), 2);
        assert_eq!(get_operator_precedence(&TokenType::MULTIPLY), 3);
        assert_eq!(get_operator_precedence(&TokenType::SELECT), 0); // Not an operator for expressions
        assert_eq!(get_operator_precedence(&TokenType::LeftParen), 0); // Not an infix operator
    }
} 