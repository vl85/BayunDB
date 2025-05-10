use anyhow::{Result, anyhow};
use bayundb::query::parser::parse;
use bayundb::query::parser::ast::{Statement, Expression, Operator};

#[test]
fn test_simple_select_query() -> Result<()> {
    // Test basic parser functionality
    let sql = "SELECT id, name FROM test_table WHERE id > 5";
    
    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
    // Verify statement structure
    if let Statement::Select(select) = statement {
        assert_eq!(select.columns.len(), 2);
        assert_eq!(select.from.len(), 1);
        assert_eq!(select.from[0].name, "test_table");
        assert!(select.where_clause.is_some(), "Expected WHERE clause");
        
        // Check the where clause
        if let Some(where_clause) = select.where_clause {
            // Should be a binary operation with > operator
            if let Expression::BinaryOp { op, .. } = *where_clause {
                assert_eq!(op, Operator::GreaterThan);
            } else {
                panic!("Expected binary operation in WHERE clause");
            }
        }
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

#[test]
fn test_create_table_statement() -> Result<()> {
    // Test CREATE TABLE parser functionality
    let sql = "CREATE TABLE users (id INTEGER, name TEXT, active BOOLEAN)";
    
    // CREATE TABLE is now implemented, so we expect success
    let result = parse(sql);
    
    // The parser should return a valid CREATE statement
    assert!(result.is_ok(), "CREATE TABLE should be successfully parsed");
    
    if let Ok(Statement::Create(create_stmt)) = result {
        assert_eq!(create_stmt.table_name, "users");
        assert_eq!(create_stmt.columns.len(), 3);
        
        // Check first column
        assert_eq!(create_stmt.columns[0].name, "id");
        
        // Check second column
        assert_eq!(create_stmt.columns[1].name, "name");
        
        // Check third column
        assert_eq!(create_stmt.columns[2].name, "active");
    } else {
        panic!("Expected CREATE TABLE statement");
    }
    
    Ok(())
}

#[test]
fn test_complex_select_query() -> Result<()> {
    // Test more complex query with multiple conditions
    let sql = "SELECT id, name, value FROM products WHERE price > 100 AND category = 'electronics'";
    
    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
    // Verify statement structure
    if let Statement::Select(select) = statement {
        assert_eq!(select.columns.len(), 3);
        assert_eq!(select.from.len(), 1);
        assert_eq!(select.from[0].name, "products");
        assert!(select.where_clause.is_some(), "Expected WHERE clause");
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

// Test error handling - Parse errors
#[test]
fn test_syntax_error() -> Result<()> {
    // Test invalid SQL that should fail parsing
    let sql = "SELCT id FROM table"; // Misspelled SELECT
    
    let result = parse(sql);
    assert!(result.is_err(), "Expected parser to fail with syntax error");
    
    Ok(())
}

// Test error handling - Semantic errors
#[test]
fn test_invalid_predicate() -> Result<()> {
    // Test query with invalid predicate (comparing different types)
    let sql = "SELECT id FROM test_table WHERE id = 'string'";
    
    // Parsing should succeed (it's syntactically valid)
    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
    if let Statement::Select(select) = statement {
        // We just verify we can parse it - actual type checking would happen later
        assert!(select.where_clause.is_some(), "Expected WHERE clause");
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

// Test error handling for CREATE TABLE
#[test]
fn test_invalid_create_table() -> Result<()> {
    // Test CREATE TABLE with syntax error
    let sql = "CREATE TABLE users id INTEGER, name TEXT)"; // Missing opening parenthesis
    
    let result = parse(sql);
    assert!(result.is_err(), "Expected parser to fail with syntax error");
    
    Ok(())
} 