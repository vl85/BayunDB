// SQL Parser Components
//
// This module contains the separate components of the SQL parser.
// Each component handles a specific aspect of SQL parsing.

// Core parser component
pub mod parser_core;
pub mod parser_expressions;
pub mod parser_select;
pub mod parser_ddl;
pub mod parser_dml;

// Re-export frequently used items
pub use parser_core::{Parser, ParseResult, ParseError};
pub use parser_expressions::parse_expression;
pub use parser_select::parse_select;
pub use parser_ddl::{parse_create, parse_drop, parse_alter};
pub use parser_dml::{parse_insert, parse_update, parse_delete};

// We'll incrementally add the other parser components in separate modules
// mod parser_dml; 