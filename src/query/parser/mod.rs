// SQL Parser Module
//
// This module is responsible for parsing SQL queries and converting them
// into an abstract syntax tree (AST) representation.

// Re-export public components
pub mod lexer;
pub mod ast;
pub mod parser;
pub mod components;

// Export key types
pub use self::parser::parse;
pub use self::lexer::Lexer;
pub use self::lexer::Token;
pub use self::ast::Statement;
// Re-export the parser type from components for backward compatibility
pub use self::components::Parser; 