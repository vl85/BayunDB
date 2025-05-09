// SQL Lexer Implementation
//
// This module implements a lexer for SQL that tokenizes input queries.

use std::fmt;
use std::iter::Peekable;
use std::str::Chars;

/// SQL Token types
#[derive(Debug, PartialEq, Clone)]
pub enum TokenType {
    // Keywords
    SELECT,
    FROM,
    WHERE,
    INSERT,
    INTO,
    VALUES,
    DELETE,
    UPDATE,
    SET,
    CREATE,
    TABLE,
    INDEX,
    DROP,
    ALTER,
    ADD,
    JOIN,
    INNER,
    LEFT,
    RIGHT,
    FULL,
    OUTER,
    CROSS,
    ON,
    GROUP,
    BY,
    HAVING,
    COUNT,
    SUM,
    AVG,
    MIN,
    MAX,
    AS,
    
    // Literals
    STRING(String),
    INTEGER(i64),
    FLOAT(f64),
    
    // Identifiers
    IDENTIFIER(String),
    
    // Operators
    EQUALS,         // =
    LessThan,       // <
    GreaterThan,    // >
    LessEqual,      // <=
    GreaterEqual,   // >=
    NotEqual,       // <>
    PLUS,           // +
    MINUS,          // -
    MULTIPLY,       // *
    DIVIDE,         // /
    MODULO,         // %
    
    // Punctuation
    SEMICOLON,      // ;
    COMMA,          // ,
    LeftParen,      // (
    RightParen,     // )
    DOT,            // .
    
    // Special
    EOF,
    ILLEGAL(String),
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
            if is_letter(next_ch) || next_ch.is_digit(10) || next_ch == '_' {
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
            if ch.is_digit(10) {
                number.push(ch);
            }
        }
        
        // Read rest of the number
        while let Some(next_ch) = self.peek_char() {
            if next_ch.is_digit(10) {
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
            _ => TokenType::IDENTIFIER(ident.to_string()),
        }
    }
    
    /// Get the next token from the input
    pub fn next_token(&mut self) -> Token {
        self.skip_whitespace();
        
        let mut token = Token {
            token_type: TokenType::EOF,
            literal: String::new(),
            line: self.line,
            column: self.column,
        };
        
        match self.ch {
            Some(ch) => {
                token.literal = ch.to_string();
                
                match ch {
                    ';' => token.token_type = TokenType::SEMICOLON,
                    ',' => token.token_type = TokenType::COMMA,
                    '(' => token.token_type = TokenType::LeftParen,
                    ')' => token.token_type = TokenType::RightParen,
                    '.' => token.token_type = TokenType::DOT,
                    '+' => token.token_type = TokenType::PLUS,
                    '-' => token.token_type = TokenType::MINUS,
                    '*' => token.token_type = TokenType::MULTIPLY,
                    '/' => token.token_type = TokenType::DIVIDE,
                    '%' => token.token_type = TokenType::MODULO,
                    '=' => token.token_type = TokenType::EQUALS,
                    '<' => {
                        if let Some(next_ch) = self.peek_char() {
                            if next_ch == '=' {
                                self.read_char();
                                token.literal.push('=');
                                token.token_type = TokenType::LessEqual;
                            } else if next_ch == '>' {
                                self.read_char();
                                token.literal.push('>');
                                token.token_type = TokenType::NotEqual;
                            } else {
                                token.token_type = TokenType::LessThan;
                            }
                        } else {
                            token.token_type = TokenType::LessThan;
                        }
                    },
                    '>' => {
                        if let Some(next_ch) = self.peek_char() {
                            if next_ch == '=' {
                                self.read_char();
                                token.literal.push('=');
                                token.token_type = TokenType::GreaterEqual;
                            } else {
                                token.token_type = TokenType::GreaterThan;
                            }
                        } else {
                            token.token_type = TokenType::GreaterThan;
                        }
                    },
                    '\'' => {
                        let str_value = self.read_string();
                        token.literal = format!("'{}'", str_value);
                        token.token_type = TokenType::STRING(str_value);
                        return token; // No need to advance since read_string handled it
                    },
                    _ => {
                        if is_letter(ch) {
                            let identifier = self.read_identifier();
                            token.literal = identifier.clone();
                            token.token_type = self.lookup_identifier(&identifier);
                            return token; // No need to read_char again
                        } else if ch.is_digit(10) {
                            let number = self.read_number();
                            token.literal = number.clone();
                            
                            // Determine if it's an integer or float
                            if number.contains('.') {
                                if let Ok(value) = number.parse::<f64>() {
                                    token.token_type = TokenType::FLOAT(value);
                                } else {
                                    token.token_type = TokenType::ILLEGAL(number);
                                }
                            } else {
                                if let Ok(value) = number.parse::<i64>() {
                                    token.token_type = TokenType::INTEGER(value);
                                } else {
                                    token.token_type = TokenType::ILLEGAL(number);
                                }
                            }
                            return token; // No need to read_char again
                        } else {
                            token.token_type = TokenType::ILLEGAL(ch.to_string());
                        }
                    }
                }
            },
            None => {
                token.token_type = TokenType::EOF;
                token.literal = "".to_string();
                return token;
            }
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
        
        let expected_tokens = vec![
            TokenType::SELECT,
            TokenType::MULTIPLY,
            TokenType::FROM,
            TokenType::IDENTIFIER("users".to_string()),
            TokenType::WHERE,
            TokenType::IDENTIFIER("id".to_string()),
            TokenType::EQUALS,
            TokenType::INTEGER(1),
            TokenType::SEMICOLON,
            TokenType::EOF,
        ];
        
        for expected in expected_tokens {
            let token = lexer.next_token();
            assert_eq!(token.token_type, expected);
        }
    }
    
    #[test]
    fn test_complex_query() {
        let input = "SELECT name, age FROM users WHERE age > 18 AND name <> 'Bob';";
        let mut lexer = Lexer::new(input);
        
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::SELECT);
        
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::IDENTIFIER("name".to_string()));
        
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::COMMA);
        
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::IDENTIFIER("age".to_string()));
        
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::FROM);
        
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::IDENTIFIER("users".to_string()));
        
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::WHERE);
        
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::IDENTIFIER("age".to_string()));
        
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::GreaterThan);
        
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::INTEGER(18));
        
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::IDENTIFIER("AND".to_string()));
        
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::IDENTIFIER("name".to_string()));
        
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::NotEqual);
        
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::STRING("Bob".to_string()));
        
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::SEMICOLON);
        
        let token = lexer.next_token();
        assert_eq!(token.token_type, TokenType::EOF);
    }
} 