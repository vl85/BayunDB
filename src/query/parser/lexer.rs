// SQL Lexer Implementation
//
// This module implements a lexer for SQL that tokenizes input queries.

use std::fmt;
use std::iter::Peekable;
use std::str::Chars;

/// SQL Token types
#[derive(Debug, PartialEq, Clone)]
pub enum TokenType {
    ILLEGAL, EOF,

    // Identifiers + literals
    IDENTIFIER(String), // Holds the identifier string
    STRING(String),     // Holds the string value, without quotes
    // Numeric literals are first tokenized as generic NUMBER, then refined or stored as string in Token.literal
    // For simplicity with current errors, let's assume INTEGER and FLOAT are distinct types recognized by lexer, 
    // but they don't carry the value directly in TokenType enum.
    INTEGER, // Unit variant, value in Token.literal
    FLOAT,   // Unit variant, value in Token.literal

    // Operators
    // ... (ASSIGN, PLUS, MINUS, etc.)
    ASSIGN, PLUS, MINUS, BANG, ASTERISK, SLASH, PERCENT,
    LT, GT, EQ, 
    NotEq, // Was NOT_EQ
    LtEq,  // Was LT_EQ
    GtEq,  // Was GT_EQ

    // Delimiters
    // ... (COMMA, SEMICOLON, LPAREN, etc.)
    COMMA, SEMICOLON, LPAREN, RPAREN, LBRACE, RBRACE, LBRACKET, RBRACKET, DOT,

    // Keywords
    FUNCTION, LET, TRUE, FALSE, IF, ELSE, RETURN, WHILE, FOR, IN, AS, 
    SELECT, FROM, WHERE, JOIN, INNER, LEFT, RIGHT, FULL, CROSS, ON, OUTER,
    GROUP, HAVING, INSERT, INTO, VALUES, UPDATE, SET, DELETE,
    CREATE, TABLE, ALTER, ADD, DROP, COLUMN, RENAME, TO, TYPE, 
    TEXT, BOOLEAN, DATE, TIMESTAMP, // Removed INTEGER, FLOAT from here as they are above
    PRIMARY, KEY, NULL, NOT, 
    DEFAULT, UNIQUE, CHECK, CONSTRAINT, FOREIGN, REFERENCES, 
    INDEX, 
    ORDER, BY, ASC, DESC, // Added for ORDER BY
    COUNT, SUM, AVG, MIN, MAX, // Aggregate functions
    CASE, WHEN, THEN, END, IS, // Added IS
    // Any other keywords...
}

/// A Token represents a lexical unit in the SQL query
#[derive(Debug, Clone)]
pub struct Token {
    pub token_type: TokenType,
    pub literal: String,
    pub line: usize,
    pub column: usize,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}({})", self.token_type, self.literal)
    }
}

/// SQL Lexer for breaking a query string into tokens
pub struct Lexer<'a> {
    input: Peekable<Chars<'a>>,
    line: usize,
    column: usize,
    ch: Option<char>,
}

impl<'a> Lexer<'a> {
    /// Create a new lexer from a SQL query string
    pub fn new(input: &'a str) -> Self {
        let mut lexer = Lexer {
            input: input.chars().peekable(),
            line: 1,
            column: 0,
            ch: None,
        };
        lexer.read_char();
        lexer
    }
    
    /// Read the next character from the input
    fn read_char(&mut self) -> Option<char> {
        let ch = self.input.next();
        self.ch = ch;
        
        if let Some(c) = ch {
            self.column += 1;
            if c == '\n' {
                self.line += 1;
                self.column = 0;
            }
        }
        
        ch
    }
    
    /// Peek at the next character without advancing
    fn peek_char(&mut self) -> Option<char> {
        self.input.peek().copied()
    }
    
    /// Skip whitespace characters
    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.ch {
            if ch.is_whitespace() {
                self.read_char();
            } else {
                break;
            }
        }
    }
    
    /// Read an identifier or keyword
    fn read_identifier(&mut self) -> String {
        let mut identifier = String::new();
        
        // First character is already read in self.ch
        if let Some(ch) = self.ch {
            if is_letter(ch) {
                identifier.push(ch);
            }
        }
        
        // Read rest of identifier
        while let Some(next_ch) = self.peek_char() {
            if is_letter(next_ch) || next_ch.is_ascii_digit() || next_ch == '_' {
                identifier.push(next_ch);
                self.read_char();
            } else {
                break;
            }
        }
        
        // Advance past the identifier
        self.read_char();
        
        identifier
    }
    
    /// Read a number (integer or float)
    fn read_number(&mut self) -> String {
        let mut number = String::new();
        let mut has_dot = false;
        
        // First digit is already read in self.ch
        if let Some(ch) = self.ch {
            if ch.is_ascii_digit() {
                number.push(ch);
            }
        }
        
        // Read rest of the number
        while let Some(next_ch) = self.peek_char() {
            if next_ch.is_ascii_digit() {
                number.push(next_ch);
                self.read_char();
            } else if next_ch == '.' && !has_dot {
                has_dot = true;
                number.push(next_ch);
                self.read_char();
            } else {
                break;
            }
        }
        
        // Advance past the number
        self.read_char();
        
        number
    }
    
    /// Read a string literal (enclosed in quotes)
    fn read_string(&mut self) -> String {
        let mut string = String::new();
        
        // Skip opening quote which is in self.ch
        self.read_char();
        
        while let Some(ch) = self.ch {
            if ch == '\'' {
                // Skip closing quote
                self.read_char();
                break;
            } else {
                string.push(ch);
                self.read_char();
            }
        }
        
        string
    }
    
    /// Get the token type for an identifier (could be a keyword)
    fn lookup_identifier(&self, ident: &str) -> TokenType {
        match ident.to_uppercase().as_str() {
            "SELECT" => TokenType::SELECT,
            "FROM" => TokenType::FROM,
            "WHERE" => TokenType::WHERE,
            "INSERT" => TokenType::INSERT,
            "INTO" => TokenType::INTO,
            "VALUES" => TokenType::VALUES,
            "DELETE" => TokenType::DELETE,
            "UPDATE" => TokenType::UPDATE,
            "SET" => TokenType::SET,
            "CREATE" => TokenType::CREATE,
            "TABLE" => TokenType::TABLE,
            "INDEX" => TokenType::INDEX,
            "DROP" => TokenType::DROP,
            "ALTER" => TokenType::ALTER,
            "ADD" => TokenType::ADD,
            "COLUMN" => TokenType::COLUMN,
            "RENAME" => TokenType::RENAME,
            "TO" => TokenType::TO,
            "JOIN" => TokenType::JOIN,
            "INNER" => TokenType::INNER,
            "LEFT" => TokenType::LEFT,
            "RIGHT" => TokenType::RIGHT,
            "FULL" => TokenType::FULL,
            "OUTER" => TokenType::OUTER,
            "CROSS" => TokenType::CROSS,
            "ON" => TokenType::ON,
            "GROUP" => TokenType::GROUP,
            "BY" => TokenType::BY,
            "HAVING" => TokenType::HAVING,
            "COUNT" => TokenType::COUNT,
            "SUM" => TokenType::SUM,
            "AVG" => TokenType::AVG,
            "MIN" => TokenType::MIN,
            "MAX" => TokenType::MAX,
            "AS" => TokenType::AS,
            "TYPE" => TokenType::TYPE,
            "TEXT" => TokenType::TEXT,
            "BOOLEAN" => TokenType::BOOLEAN,
            "DATE" => TokenType::DATE,
            "TIMESTAMP" => TokenType::TIMESTAMP,
            "PRIMARY" => TokenType::PRIMARY,
            "KEY" => TokenType::KEY,
            "NULL" => TokenType::NULL,
            "NOT" => TokenType::NOT,
            "DEFAULT" => TokenType::DEFAULT,
            "UNIQUE" => TokenType::UNIQUE,
            "CHECK" => TokenType::CHECK,
            "CONSTRAINT" => TokenType::CONSTRAINT,
            "FOREIGN" => TokenType::FOREIGN,
            "REFERENCES" => TokenType::REFERENCES,
            "ORDER" => TokenType::ORDER,
            "ASC" => TokenType::ASC,
            "DESC" => TokenType::DESC,
            "CASE" => TokenType::CASE,
            "WHEN" => TokenType::WHEN,
            "THEN" => TokenType::THEN,
            "ELSE" => TokenType::ELSE,
            "END" => TokenType::END,
            "IS" => TokenType::IS,
            _ => TokenType::IDENTIFIER(ident.to_string()),
        }
    }
    
    /// Get the next token from the input
    pub fn next_token(&mut self) -> Token {
        self.skip_whitespace();
        
        let mut token = Token {
            token_type: TokenType::ILLEGAL,
            literal: "".to_string(),
            line: self.line,
            column: self.column,
        };
        
        if let Some(ch) = self.ch {
            token.literal = ch.to_string(); // Default literal is the char itself for single char tokens
            
            match ch {
                ';' => token.token_type = TokenType::SEMICOLON,
                ',' => token.token_type = TokenType::COMMA,
                '(' => token.token_type = TokenType::LPAREN,
                ')' => token.token_type = TokenType::RPAREN,
                '.' => token.token_type = TokenType::DOT,
                '+' => token.token_type = TokenType::PLUS,
                '-' => token.token_type = TokenType::MINUS,
                '*' => token.token_type = TokenType::ASTERISK,
                '/' => token.token_type = TokenType::SLASH,
                '%' => token.token_type = TokenType::PERCENT,
                '=' => token.token_type = TokenType::ASSIGN,
                '<' => {
                    if let Some(next_ch) = self.peek_char() {
                        if next_ch == '=' {
                            self.read_char();
                            token.literal.push('=');
                            token.token_type = TokenType::LtEq;
                        } else if next_ch == '>' {
                            self.read_char();
                            token.literal.push('>');
                            token.token_type = TokenType::NotEq;
                        } else {
                            token.token_type = TokenType::LT;
                        }
                    } else {
                        token.token_type = TokenType::LT;
                    }
                },
                '>' => {
                    if let Some(next_ch) = self.peek_char() {
                        if next_ch == '=' {
                            self.read_char();
                            token.literal.push('=');
                            token.token_type = TokenType::GtEq;
                        } else {
                            token.token_type = TokenType::GT;
                        }
                    } else {
                        token.token_type = TokenType::GT;
                    }
                },
                '\'' => {
                    token.literal = self.read_string();
                    token.token_type = TokenType::STRING(token.literal.clone());
                    return token;
                },
                _ => {
                    if is_letter(ch) {
                        let identifier = self.read_identifier();
                        token.token_type = self.lookup_identifier(&identifier);
                        token.literal = identifier;
                        return token;
                    } else if ch.is_ascii_digit() {
                        let number_str = self.read_number();
                        token.literal = number_str.clone();
                        if number_str.contains('.') {
                            if number_str.parse::<f64>().is_ok() {
                                token.token_type = TokenType::FLOAT;
                            } else {
                                token.token_type = TokenType::ILLEGAL;
                            }
                        } else if number_str.parse::<i64>().is_ok() {
                            token.token_type = TokenType::INTEGER;
                        } else {
                            token.token_type = TokenType::ILLEGAL;
                        }
                        return token;
                    } else {
                        // Unrecognized character, already set to ILLEGAL with ch as literal
                    }
                }
            }
        } else {
            token.token_type = TokenType::EOF;
            token.literal = "".to_string();
        }
        
        self.read_char();
        token
    }
}

/// Check if a character is a letter (for identifiers)
fn is_letter(ch: char) -> bool {
    ch.is_alphabetic() || ch == '_'
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_simple_tokens() {
        let input = "SELECT * FROM users WHERE id = 1;";
        let mut lexer = Lexer::new(input);
        
        let expected_tokens_types_literals = vec![
            (TokenType::SELECT, "SELECT"),
            (TokenType::ASTERISK, "*"),
            (TokenType::FROM, "FROM"),
            (TokenType::IDENTIFIER("users".to_string()), "users"),
            (TokenType::WHERE, "WHERE"),
            (TokenType::IDENTIFIER("id".to_string()), "id"),
            (TokenType::ASSIGN, "="),
            (TokenType::INTEGER, "1"),
            (TokenType::SEMICOLON, ";"),
            (TokenType::EOF, ""),
        ];
        
        for (expected_type, expected_literal) in expected_tokens_types_literals {
            let token = lexer.next_token();
            assert_eq!(token.token_type, expected_type, "Token type mismatch for literal '{}'", token.literal);
            assert_eq!(token.literal, expected_literal, "Token literal mismatch for type {:?}", token.token_type);
        }
    }
    
    #[test]
    fn test_complex_query() {
        let input = "SELECT name, age FROM users WHERE age > 18 AND name <> 'Bob';";
        let mut lexer = Lexer::new(input);
        
        // Helper to check next token
        let mut assert_next_token = |expected_type: TokenType, expected_literal: &str| {
            let token = lexer.next_token();
            assert_eq!(token.token_type, expected_type, "Type check failed for literal: {}", token.literal);
            assert_eq!(token.literal, expected_literal, "Literal check failed for type: {:?}", token.token_type);
        };

        assert_next_token(TokenType::SELECT, "SELECT");
        assert_next_token(TokenType::IDENTIFIER("name".to_string()), "name");
        assert_next_token(TokenType::COMMA, ",");
        assert_next_token(TokenType::IDENTIFIER("age".to_string()), "age");
        assert_next_token(TokenType::FROM, "FROM");
        assert_next_token(TokenType::IDENTIFIER("users".to_string()), "users");
        assert_next_token(TokenType::WHERE, "WHERE");
        assert_next_token(TokenType::IDENTIFIER("age".to_string()), "age");
        assert_next_token(TokenType::GT, ">");
        assert_next_token(TokenType::INTEGER, "18");
        assert_next_token(TokenType::IDENTIFIER("AND".to_string()), "AND");
        assert_next_token(TokenType::IDENTIFIER("name".to_string()), "name");
        assert_next_token(TokenType::NotEq, "<>");
        assert_next_token(TokenType::STRING("Bob".to_string()), "Bob");
        assert_next_token(TokenType::SEMICOLON, ";");
        assert_next_token(TokenType::EOF, "");
    }
} 