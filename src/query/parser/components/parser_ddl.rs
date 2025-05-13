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
    parser.expect_token(TokenType::LPAREN)?;
    
    // Parse column definitions
    let columns = parse_column_definitions(parser)?;
    
    // Expect closing parenthesis
    parser.expect_token(TokenType::RPAREN)?;
    
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
        // Optional: check for COLUMN keyword
        if parser.current_token_is(TokenType::COLUMN) {
            parser.next_token(); // Consume COLUMN
        }
        // Parse column definition (reuse CREATE TABLE logic)
        let col_def = {
            let name = parser.parse_identifier()?;
            let data_type = parse_data_type(parser)?;
            // Parse optional constraints
            let mut nullable = true;
            let mut primary_key = false;
            let mut default_value = None;
            loop { // Changed from while let Some(token)
                if parser.current_token_is(TokenType::NOT) {
                    parser.next_token(); // Consume NOT
                    parser.expect_token(TokenType::NULL)?; // Expect NULL
                    nullable = false;
                } else if parser.current_token_is(TokenType::PRIMARY) {
                    parser.next_token(); // Consume PRIMARY
                    parser.expect_token(TokenType::KEY)?; // Expect KEY
                    primary_key = true;
                    nullable = false; // Primary keys are implicitly NOT NULL
                } else if parser.current_token_is(TokenType::DEFAULT) {
                    parser.next_token(); // Consume DEFAULT
                    let expr = super::parser_expressions::parse_expression(parser, 0)?;
                    default_value = Some(expr);
                } else {
                    break; // Not a known constraint keyword
                }
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
    } else if parser.current_token_is(TokenType::ALTER) {
        parser.next_token(); // Consume ALTER
        parser.expect_token(TokenType::COLUMN)?;
        let column_name = parser.parse_identifier()?;
        
        // Expect TYPE keyword
        parser.expect_token(TokenType::TYPE)?;
        
        // Now current_token should be pointing at the start of the data type.
        let new_type = parse_data_type(parser)?;
        if parser.current_token_is(TokenType::SEMICOLON) { parser.next_token(); }
        Ok(Statement::Alter(AlterTableStatement {
            table_name,
            operation: AlterTableOperation::AlterColumnType { column_name, new_type },
        }))
    } else {
        Err(ParseError::InvalidSyntax("Expected ADD, DROP, RENAME, or ALTER after ALTER TABLE <table>".to_string()))
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
        
        loop { // Changed from while let Some(token)
            if parser.current_token_is(TokenType::NOT) {
                parser.next_token(); // Consume NOT
                parser.expect_token(TokenType::NULL)?; // Expect NULL
                nullable = false;
            } else if parser.current_token_is(TokenType::PRIMARY) {
                parser.next_token(); // Consume PRIMARY
                parser.expect_token(TokenType::KEY)?; // Expect KEY
                primary_key = true;
                nullable = false; // Primary keys are implicitly NOT NULL
            } else if parser.current_token_is(TokenType::DEFAULT) {
                parser.next_token(); // Consume DEFAULT
                let expr = super::parser_expressions::parse_expression(parser, 0)?;
                default_value = Some(expr);
            } else {
                break; // Not a known constraint keyword
            }
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
    let token_literal = parser.current_token_literal_or_err()?;
    let type_name_upper = token_literal.to_uppercase();

    // Determine base type
    let base_data_type = match type_name_upper.as_str() {
        "INTEGER" | "INT" => Ok(DataType::Integer),
        "FLOAT" | "REAL" => Ok(DataType::Float),
        "TEXT" | "VARCHAR" | "CHAR" => Ok(DataType::Text), // Will handle (size) after this match
        "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
        "DATE" => Ok(DataType::Date),
        "TIME" => Ok(DataType::Time),
        "TIMESTAMP" => Ok(DataType::Timestamp),
        _ => Err(ParseError::InvalidSyntax(format!("Unknown data type: {}", token_literal))),
    }?;

    parser.next_token(); // Consume the token that formed the base type name

    // Handle optional (size) parameter for text types AFTER consuming base type token
    // This check needs to be specific to types that accept size parameters.
    if (type_name_upper == "TEXT" || type_name_upper == "VARCHAR" || type_name_upper == "CHAR") 
        && parser.current_token_is(TokenType::LPAREN) {
        parser.next_token(); // Consume LPAREN
        if parser.current_token_is(TokenType::INTEGER) {
            // let _size_lit = parser.current_token_literal_or_err()?; // If needed for validation/use
            parser.next_token(); // Consume INTEGER size
        } else {
            return Err(ParseError::InvalidSyntax(format!("Expected integer size for {} type parameter", type_name_upper)));
        }
        parser.expect_token(TokenType::RPAREN)?;
    }
    
    Ok(base_data_type)
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
            assert!(id_col.primary_key);
            assert!(!id_col.nullable); // Primary keys should be non-nullable
            
            // Check name column
            let name_col = &create_stmt.columns[1];
            assert_eq!(name_col.name, "name");
            assert_eq!(name_col.data_type, DataType::Text);
            assert!(!name_col.primary_key);
            assert!(!name_col.nullable);
            
            // Check email column 
            let email_col = &create_stmt.columns[2];
            assert_eq!(email_col.name, "email");
            assert_eq!(email_col.data_type, DataType::Text);
            assert!(!email_col.primary_key);
            assert!(email_col.nullable);
            
            // Check created_at column
            let created_col = &create_stmt.columns[3];
            assert_eq!(created_col.name, "created_at");
            assert_eq!(created_col.data_type, DataType::Timestamp);
            assert!(!created_col.primary_key);
            assert!(created_col.nullable);
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
        // parse_data_type expects an IDENTIFIER for type names.
        // Parsing a raw number like "123" as a type name should fail.
        let mut parser_int_literal = Parser::new("123"); 
        let dt_int_result = parse_data_type(&mut parser_int_literal);
        assert!(dt_int_result.is_err(), "Parsing '123' as a data type name should fail.");
        if let Err(ParseError::InvalidSyntax(msg)) = dt_int_result {
            assert!(msg.contains("Unknown data type: 123"), "Error message: {}", msg);
        } else {
            panic!("Expected InvalidSyntax for '123', got {:?}", dt_int_result);
        }

        let mut parser_float_literal = Parser::new("1.23"); 
        let dt_float_result = parse_data_type(&mut parser_float_literal);
        assert!(dt_float_result.is_err(), "Parsing '1.23' as a data type name should fail.");
        if let Err(ParseError::InvalidSyntax(msg)) = dt_float_result {
            assert!(msg.contains("Unknown data type: 1.23"), "Error message: {}", msg);
        } else {
            panic!("Expected InvalidSyntax for '1.23', got {:?}", dt_float_result);
        }
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
                    assert!(!col.nullable);
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
        let sql = "ALTER TABLE users ADD COLUMN created_at TIMESTAMP DEFAULT NOW()";
        let mut parser = Parser::new(sql);
        let result = parse_alter(&mut parser);
        assert!(result.is_ok());
        if let Statement::Alter(AlterTableStatement { table_name, operation }) = result.unwrap() {
            assert_eq!(table_name, "users");
            if let AlterTableOperation::AddColumn(col_def) = operation {
                assert_eq!(col_def.name, "created_at");
                assert_eq!(col_def.data_type, DataType::Timestamp);
                assert!(col_def.default_value.is_some());
            } else {
                panic!("Expected AddColumn operation");
            }
        } else {
            panic!("Expected AlterTableStatement");
        }
    }

    #[test]
    fn test_parse_alter_table_alter_column_type() {
        let sql = "ALTER TABLE products ALTER COLUMN price TYPE FLOAT";
        let mut parser = Parser::new(sql);
        let result = parse_alter(&mut parser);
        assert!(result.is_ok(), "Parse failed: {:?}", result.err());
        if let Statement::Alter(AlterTableStatement { table_name, operation }) = result.unwrap() {
            assert_eq!(table_name, "products");
            if let AlterTableOperation::AlterColumnType { column_name, new_type } = operation {
                assert_eq!(column_name, "price");
                assert_eq!(new_type, DataType::Float);
            } else {
                panic!("Expected AlterColumnType operation, got {:?}", operation);
            }
        } else {
            panic!("Expected AlterTableStatement");
        }
    }

    #[test]
    fn test_parse_alter_table_alter_column_type_case_insensitive() {
        let sql = "alter table My_Table alter column Old_Name type InTeGeR";
        let mut parser = Parser::new(sql);
        let result = parse_alter(&mut parser);
        assert!(result.is_ok(), "Parse failed: {:?}", result.err());
        if let Statement::Alter(AlterTableStatement { table_name, operation }) = result.unwrap() {
            assert_eq!(table_name, "My_Table");
            if let AlterTableOperation::AlterColumnType { column_name, new_type } = operation {
                assert_eq!(column_name, "Old_Name");
                assert_eq!(new_type, DataType::Integer);
            } else {
                panic!("Expected AlterColumnType operation, got {:?}", operation);
            }
        } else {
            panic!("Expected AlterTableStatement");
        }
    }

    #[test]
    fn test_parse_alter_table_alter_column_type_invalid_syntax() {
        let sql_missing_type_keyword = "ALTER TABLE products ALTER COLUMN price FLOAT";
        let mut parser_missing_type = Parser::new(sql_missing_type_keyword);
        let result_missing_type = parse_alter(&mut parser_missing_type);
        assert!(result_missing_type.is_err());
        if let Err(ParseError::ExpectedToken(expected_tt, found_token)) = result_missing_type {
            assert_eq!(expected_tt, TokenType::TYPE, "Expected TokenType::TYPE to be missing");
            assert_eq!(found_token.token_type, TokenType::IDENTIFIER("FLOAT".to_string()), "Found token should be IDENTIFIER(\"FLOAT\")");
        } else {
            panic!("Expected ParseError::ExpectedToken(TYPE, IDENTIFIER(\"FLOAT\")) error for missing TYPE keyword, got {:?}", result_missing_type);
        }

        let sql_missing_new_type = "ALTER TABLE products ALTER COLUMN price TYPE";
        let mut parser_missing_new_type = Parser::new(sql_missing_new_type);
        let result_missing_new_type = parse_alter(&mut parser_missing_new_type);
        assert!(result_missing_new_type.is_err());
        // If TYPE is the last token, parse_data_type will be called with current_token being EOF.
        // current_token_literal_or_err() will yield Ok("").
        // parse_data_type will then produce InvalidSyntax("Unknown data type: ")
        if let Err(ParseError::InvalidSyntax(msg)) = result_missing_new_type {
            assert_eq!(msg, "Unknown data type: ", "Error message mismatch for missing new data type");
        } else {
            panic!("Expected InvalidSyntax(\"Unknown data type: \") for missing new data type, got {:?}", result_missing_new_type);
        }
    }
} 