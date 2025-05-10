// DDL Statement Parser Implementation
//
// This module implements parsing for SQL DDL (Data Definition Language) statements:
// CREATE, ALTER, and DROP.

use crate::query::parser::ast::*;
use crate::query::parser::lexer::TokenType;
use super::parser_core::{Parser, ParseResult, ParseError};

/// Parse a CREATE statement
pub fn parse_create(parser: &mut Parser) -> ParseResult<Statement> {
    // Consume CREATE keyword
    parser.expect_token(TokenType::CREATE)?;
    
    // Check what we're creating
    if parser.current_token_is(TokenType::TABLE) {
        parse_create_table(parser)
    } else {
        Err(ParseError::InvalidSyntax("Only CREATE TABLE is supported".to_string()))
    }
}

/// Parse a CREATE TABLE statement
fn parse_create_table(parser: &mut Parser) -> ParseResult<Statement> {
    // Consume TABLE keyword
    parser.expect_token(TokenType::TABLE)?;
    
    // Parse table name
    let table_name = parser.parse_identifier()?;
    
    // Expect opening parenthesis
    parser.expect_token(TokenType::LeftParen)?;
    
    // Parse column definitions
    let columns = parse_column_definitions(parser)?;
    
    // Expect closing parenthesis
    parser.expect_token(TokenType::RightParen)?;
    
    // Optional semicolon
    if parser.current_token_is(TokenType::SEMICOLON) {
        parser.next_token();
    }
    
    Ok(Statement::Create(CreateStatement {
        table_name,
        columns,
    }))
}

/// Parse a DROP statement
pub fn parse_drop(parser: &mut Parser) -> ParseResult<Statement> {
    // This will be implemented for DROP TABLE, DROP INDEX, etc.
    Err(ParseError::InvalidSyntax("DROP statements not implemented yet".to_string()))
}

/// Parse an ALTER statement
pub fn parse_alter(parser: &mut Parser) -> ParseResult<Statement> {
    // Consume ALTER keyword
    parser.expect_token(TokenType::ALTER)?;
    // Expect TABLE keyword
    parser.expect_token(TokenType::TABLE)?;
    // Parse table name
    let table_name = parser.parse_identifier()?;

    // Parse operation
    if parser.current_token_is(TokenType::ADD) {
        parser.next_token(); // Consume ADD
        parser.expect_token(TokenType::COLUMN)?;
        // Parse column definition (reuse CREATE TABLE logic)
        let col_def = {
            let name = parser.parse_identifier()?;
            let data_type = parse_data_type(parser)?;
            // Parse optional constraints (reuse logic from parse_column_definitions)
            let mut nullable = true;
            let mut primary_key = false;
            let mut default_value = None;
            while let Some(token) = &parser.current_token {
                if let TokenType::IDENTIFIER(constraint) = &token.token_type {
                    let constraint_str = constraint.clone();
                    if constraint_str.eq_ignore_ascii_case("NOT") {
                        parser.next_token();
                        if let Some(next_token) = &parser.current_token {
                            if let TokenType::IDENTIFIER(null_keyword) = &next_token.token_type {
                                if null_keyword.eq_ignore_ascii_case("NULL") {
                                    parser.next_token();
                                    nullable = false;
                                    continue;
                                }
                            }
                        }
                        return Err(ParseError::InvalidSyntax("Expected NULL after NOT".to_string()));
                    } else if constraint_str.eq_ignore_ascii_case("PRIMARY") {
                        parser.next_token();
                        if let Some(next_token) = &parser.current_token {
                            if let TokenType::IDENTIFIER(key_keyword) = &next_token.token_type {
                                if key_keyword.eq_ignore_ascii_case("KEY") {
                                    parser.next_token();
                                    primary_key = true;
                                    nullable = false;
                                    continue;
                                }
                            }
                        }
                        return Err(ParseError::InvalidSyntax("Expected KEY after PRIMARY".to_string()));
                    } else if constraint_str.eq_ignore_ascii_case("DEFAULT") {
                        parser.next_token();
                        let expr = super::parser_expressions::parse_expression(parser, 0)?;
                        default_value = Some(expr);
                        continue;
                    }
                }
                break;
            }
            ColumnDef { name, data_type, nullable, primary_key, default_value }
        };
        // Optional semicolon
        if parser.current_token_is(TokenType::SEMICOLON) { parser.next_token(); }
        Ok(Statement::Alter(AlterTableStatement {
            table_name,
            operation: AlterTableOperation::AddColumn(col_def),
        }))
    } else if parser.current_token_is(TokenType::DROP) {
        parser.next_token(); // Consume DROP
        parser.expect_token(TokenType::COLUMN)?;
        let col_name = parser.parse_identifier()?;
        if parser.current_token_is(TokenType::SEMICOLON) { parser.next_token(); }
        Ok(Statement::Alter(AlterTableStatement {
            table_name,
            operation: AlterTableOperation::DropColumn(col_name),
        }))
    } else if parser.current_token_is(TokenType::RENAME) {
        parser.next_token(); // Consume RENAME
        parser.expect_token(TokenType::COLUMN)?;
        let old_name = parser.parse_identifier()?;
        parser.expect_token(TokenType::TO)?;
        let new_name = parser.parse_identifier()?;
        if parser.current_token_is(TokenType::SEMICOLON) { parser.next_token(); }
        Ok(Statement::Alter(AlterTableStatement {
            table_name,
            operation: AlterTableOperation::RenameColumn { old_name, new_name },
        }))
    } else {
        Err(ParseError::InvalidSyntax("Expected ADD, DROP, or RENAME after ALTER TABLE <table>".to_string()))
    }
}

/// Parse column definitions for CREATE TABLE
fn parse_column_definitions(parser: &mut Parser) -> ParseResult<Vec<ColumnDef>> {
    let mut columns = Vec::new();
    
    loop {
        // Parse column name
        let name = parser.parse_identifier()?;
        
        // Parse data type
        let data_type = parse_data_type(parser)?;
        
        // Parse optional constraints
        let mut nullable = true;
        let mut primary_key = false;
        let mut default_value = None;
        
        while let Some(token) = &parser.current_token {
            if let TokenType::IDENTIFIER(constraint) = &token.token_type {
                let constraint_str = constraint.clone();
                if constraint_str.eq_ignore_ascii_case("NOT") {
                    parser.next_token(); // Consume NOT
                    
                    // Expect NULL identifier
                    if let Some(next_token) = &parser.current_token {
                        if let TokenType::IDENTIFIER(null_keyword) = &next_token.token_type {
                            if null_keyword.eq_ignore_ascii_case("NULL") {
                                parser.next_token(); // Consume NULL
                                nullable = false;
                                continue;
                            }
                        }
                    }
                    return Err(ParseError::InvalidSyntax("Expected NULL after NOT".to_string()));
                } else if constraint_str.eq_ignore_ascii_case("PRIMARY") {
                    parser.next_token(); // Consume PRIMARY
                    
                    // Expect KEY identifier
                    if let Some(next_token) = &parser.current_token {
                        if let TokenType::IDENTIFIER(key_keyword) = &next_token.token_type {
                            if key_keyword.eq_ignore_ascii_case("KEY") {
                                parser.next_token(); // Consume KEY
                                primary_key = true;
                                nullable = false; // Primary keys cannot be null
                                continue;
                            }
                        }
                    }
                    return Err(ParseError::InvalidSyntax("Expected KEY after PRIMARY".to_string()));
                } else if constraint_str.eq_ignore_ascii_case("DEFAULT") {
                    parser.next_token();
                    let expr = super::parser_expressions::parse_expression(parser, 0)?;
                    default_value = Some(expr);
                    continue;
                }
            }
            break; // Not a constraint, so we're done
        }
        
        columns.push(ColumnDef {
            name,
            data_type,
            nullable,
            primary_key,
            default_value,
        });
        
        // Check if we have a comma for more columns
        if parser.current_token_is(TokenType::COMMA) {
            parser.next_token(); // Consume comma
            continue;
        }
        
        break;
    }
    
    Ok(columns)
}

/// Parse a SQL data type
fn parse_data_type(parser: &mut Parser) -> ParseResult<DataType> {
    match &parser.current_token {
        Some(token) => {
            match &token.token_type {
                TokenType::INTEGER(_) => {
                    parser.next_token();
                    Ok(DataType::Integer)
                },
                TokenType::FLOAT(_) => {
                    parser.next_token();
                    Ok(DataType::Float)
                },
                TokenType::IDENTIFIER(type_name) => {
                    let type_name = type_name.clone();
                    parser.next_token();
                    
                    // Handle different data type names
                    if type_name.eq_ignore_ascii_case("INT") || 
                       type_name.eq_ignore_ascii_case("INTEGER") {
                        Ok(DataType::Integer)
                    } else if type_name.eq_ignore_ascii_case("REAL") || 
                              type_name.eq_ignore_ascii_case("FLOAT") {
                        Ok(DataType::Float)
                    } else if type_name.eq_ignore_ascii_case("TEXT") || 
                              type_name.eq_ignore_ascii_case("VARCHAR") || 
                              type_name.eq_ignore_ascii_case("CHAR") {
                        // Check for optional size parameter in parentheses
                        if parser.current_token_is(TokenType::LeftParen) {
                            parser.next_token(); // Consume left paren
                            
                            // For now, we're ignoring the size parameter
                            if let Some(token) = &parser.current_token {
                                if let TokenType::INTEGER(_) = token.token_type {
                                    parser.next_token(); // Consume the size
                                }
                            }
                            
                            parser.expect_token(TokenType::RightParen)?;
                        }
                        
                        Ok(DataType::Text)
                    } else if type_name.eq_ignore_ascii_case("BOOLEAN") ||
                              type_name.eq_ignore_ascii_case("BOOL") {
                        Ok(DataType::Boolean)
                    } else if type_name.eq_ignore_ascii_case("DATE") {
                        Ok(DataType::Date)
                    } else if type_name.eq_ignore_ascii_case("TIMESTAMP") {
                        Ok(DataType::Timestamp)
                    } else if type_name.eq_ignore_ascii_case("BLOB") ||
                              type_name.eq_ignore_ascii_case("BINARY") {
                        // Return error for unsupported BLOB type in the AST
                        Err(ParseError::InvalidSyntax(format!("Unsupported data type: {}", type_name)))
                    } else {
                        Err(ParseError::InvalidSyntax(format!("Unknown data type: {}", type_name)))
                    }
                },
                _ => Err(ParseError::UnexpectedToken(token.clone())),
            }
        },
        None => Err(ParseError::EndOfInput),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_create_table() {
        let mut parser = Parser::new("CREATE TABLE users (id INTEGER, name TEXT, active BOOLEAN)");
        
        let stmt = parse_create(&mut parser).unwrap();
        
        if let Statement::Create(create_stmt) = stmt {
            assert_eq!(create_stmt.table_name, "users");
            assert_eq!(create_stmt.columns.len(), 3);
            
            // Check first column
            assert_eq!(create_stmt.columns[0].name, "id");
            assert_eq!(create_stmt.columns[0].data_type, DataType::Integer);
            
            // Check second column
            assert_eq!(create_stmt.columns[1].name, "name");
            assert_eq!(create_stmt.columns[1].data_type, DataType::Text);
            
            // Check third column
            assert_eq!(create_stmt.columns[2].name, "active");
            assert_eq!(create_stmt.columns[2].data_type, DataType::Boolean);
        } else {
            panic!("Expected CREATE TABLE statement");
        }
    }
    
    #[test]
    fn test_parse_create_table_with_constraints() {
        let mut parser = Parser::new("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL)");
        
        let stmt = parse_create(&mut parser).unwrap();
        
        if let Statement::Create(create_stmt) = stmt {
            assert_eq!(create_stmt.table_name, "products");
            assert_eq!(create_stmt.columns.len(), 2);
            
            // Check first column with PRIMARY KEY
            assert_eq!(create_stmt.columns[0].name, "id");
            assert_eq!(create_stmt.columns[0].data_type, DataType::Integer);
            assert!(create_stmt.columns[0].primary_key);
            assert!(!create_stmt.columns[0].nullable);
            
            // Check second column with NOT NULL
            assert_eq!(create_stmt.columns[1].name, "name");
            assert_eq!(create_stmt.columns[1].data_type, DataType::Text);
            assert!(!create_stmt.columns[1].nullable);
        } else {
            panic!("Expected CREATE TABLE statement");
        }
    }
    
    #[test]
    fn test_specific_create_table() {
        let query = "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            created_at TIMESTAMP
        )";
        
        let mut parser = Parser::new(query);
        let stmt = parse_create(&mut parser).unwrap();
        
        if let Statement::Create(create_stmt) = stmt {
            // Check columns
            assert_eq!(create_stmt.columns.len(), 4);
            
            // Check id column
            let id_col = &create_stmt.columns[0];
            assert_eq!(id_col.name, "id");
            assert_eq!(id_col.data_type, DataType::Integer);
            assert_eq!(id_col.primary_key, true);
            assert_eq!(id_col.nullable, false); // Primary keys should be non-nullable
            
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

    #[test]
    fn test_parse_data_type_function() {
        let test_cases = vec![
            ("INTEGER", DataType::Integer),
            ("INT", DataType::Integer),
            ("int", DataType::Integer),
            ("FLOAT", DataType::Float),
            ("REAL", DataType::Float),
            ("real", DataType::Float),
            ("TEXT", DataType::Text),
            ("VARCHAR", DataType::Text),
            ("VARCHAR(255)", DataType::Text),
            ("char(10)", DataType::Text),
            ("BOOLEAN", DataType::Boolean),
            ("BOOL", DataType::Boolean),
            ("DATE", DataType::Date),
            ("TIMESTAMP", DataType::Timestamp),
        ];

        for (type_str, expected_type) in test_cases {
            let mut parser = Parser::new(type_str);
            let result = parse_data_type(&mut parser);
            assert!(result.is_ok(), "Failed to parse data type: {}", type_str);
            assert_eq!(result.unwrap(), expected_type, "Mismatch for type: {}", type_str);
        }

        // Test error cases
        let error_cases = vec![
            "BLOB", 
            "BINARY", 
            "UNKNOWN_TYPE",
            "VARCHAR(", // Unterminated size
            "INTEGER EXTRA_TOKEN", // Trailing token
        ];

        for type_str in error_cases {
            let mut parser = Parser::new(type_str);
            let result = parse_data_type(&mut parser);

            if type_str == "INTEGER EXTRA_TOKEN" {
                // parse_data_type successfully parses "INTEGER" and leaves "EXTRA_TOKEN".
                // This is not an error for parse_data_type itself.
                // The calling parser rule would handle "EXTRA_TOKEN".
                assert!(result.is_ok(), "parse_data_type should parse INTEGER from '{}'", type_str);
                if let Ok(data_type) = result {
                    assert_eq!(data_type, DataType::Integer, "Parsed type should be Integer for '{}'", type_str);
                }
                // Check that EXTRA_TOKEN is indeed the current token
                assert!(parser.current_token.is_some(), "EXTRA_TOKEN should remain for '{}'", type_str);
                match parser.current_token.as_ref().unwrap().token_type {
                    TokenType::IDENTIFIER(ref val) => {
                        assert_eq!(val, "EXTRA_TOKEN", "Expected IDENTIFIER to be EXTRA_TOKEN");
                    }
                    _ => panic!("Expected EXTRA_TOKEN to be an IDENTIFIER")
                }
            } else {
                assert!(result.is_err(), "Expected error for data type: {}", type_str);
            }
        }
        
        // Test parsing literal number as data type (should use IDENTIFIER path)
        // The lexer produces TokenType::INTEGER for `123` not IDENTIFIER.
        // parse_data_type expects an IDENTIFIER for type names unless it's a direct TokenType::INTEGER/FLOAT match.
        // So, `CREATE TABLE t (c1 123)` would fail at `parse_data_type` if `123` is not an IDENTIFIER.
        // However, the TokenType::INTEGER and TokenType::FLOAT arms in parse_data_type handle this.
        let mut parser_int_literal = Parser::new("123"); // Lexer makes this TokenType::INTEGER(123)
        let dt_int_result = parse_data_type(&mut parser_int_literal);
        assert!(dt_int_result.is_ok());
        assert_eq!(dt_int_result.unwrap(), DataType::Integer);

        let mut parser_float_literal = Parser::new("1.23"); // Lexer makes this TokenType::FLOAT(1.23)
        let dt_float_result = parse_data_type(&mut parser_float_literal);
        assert!(dt_float_result.is_ok());
        assert_eq!(dt_float_result.unwrap(), DataType::Float);
    }

    #[test]
    fn test_parse_alter_table_add_column() {
        let mut parser = Parser::new("ALTER TABLE users ADD COLUMN age INTEGER NOT NULL;");
        let stmt = parse_alter(&mut parser).unwrap();
        if let Statement::Alter(alter_stmt) = stmt {
            assert_eq!(alter_stmt.table_name, "users");
            match alter_stmt.operation {
                AlterTableOperation::AddColumn(ref col) => {
                    assert_eq!(col.name, "age");
                    assert_eq!(col.data_type, DataType::Integer);
                    assert_eq!(col.nullable, false);
                },
                _ => panic!("Expected AddColumn operation"),
            }
        } else {
            panic!("Expected ALTER TABLE statement");
        }
    }

    #[test]
    fn test_parse_alter_table_drop_column() {
        let mut parser = Parser::new("ALTER TABLE users DROP COLUMN age;");
        let stmt = parse_alter(&mut parser).unwrap();
        if let Statement::Alter(alter_stmt) = stmt {
            assert_eq!(alter_stmt.table_name, "users");
            match alter_stmt.operation {
                AlterTableOperation::DropColumn(ref col_name) => {
                    assert_eq!(col_name, "age");
                },
                _ => panic!("Expected DropColumn operation"),
            }
        } else {
            panic!("Expected ALTER TABLE statement");
        }
    }

    #[test]
    fn test_parse_alter_table_rename_column() {
        let mut parser = Parser::new("ALTER TABLE users RENAME COLUMN old_name TO new_name;");
        let stmt = parse_alter(&mut parser).unwrap();
        if let Statement::Alter(alter_stmt) = stmt {
            assert_eq!(alter_stmt.table_name, "users");
            match alter_stmt.operation {
                AlterTableOperation::RenameColumn { ref old_name, ref new_name } => {
                    assert_eq!(old_name, "old_name");
                    assert_eq!(new_name, "new_name");
                },
                _ => panic!("Expected RenameColumn operation"),
            }
        } else {
            panic!("Expected ALTER TABLE statement");
        }
    }

    #[test]
    fn test_parse_alter_table_add_column_with_default() {
        let mut parser = Parser::new("ALTER TABLE users ADD COLUMN age INTEGER DEFAULT 42;");
        let stmt = parse_alter(&mut parser).unwrap();
        if let Statement::Alter(alter_stmt) = stmt {
            assert_eq!(alter_stmt.table_name, "users");
            match alter_stmt.operation {
                AlterTableOperation::AddColumn(ref col) => {
                    assert_eq!(col.name, "age");
                    assert_eq!(col.data_type, DataType::Integer);
                    assert_eq!(col.nullable, true);
                    assert!(col.default_value.is_some());
                },
                _ => panic!("Expected AddColumn operation"),
            }
        } else {
            panic!("Expected ALTER TABLE statement");
        }
    }
} 