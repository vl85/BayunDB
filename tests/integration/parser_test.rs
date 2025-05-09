use anyhow::{Result, anyhow};
use bayundb::query::parser::Parser;
use bayundb::query::parser::ast::{Statement, Expression, Operator};

#[test]
fn test_simple_select_query() -> Result<()> {
    // Test basic parser functionality
    let sql = "SELECT id, name FROM test_table WHERE id > 5";
    let mut parser = Parser::new(sql);
    
    let statement = parser.parse_statement().map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
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
fn test_complex_select_query() -> Result<()> {
    // Test more complex query with multiple conditions
    let sql = "SELECT id, name, value FROM products WHERE price > 100 AND category = 'electronics'";
    let mut parser = Parser::new(sql);
    
    let statement = parser.parse_statement().map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
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
    let mut parser = Parser::new(sql);
    
    let result = parser.parse_statement();
    assert!(result.is_err(), "Expected parser to fail with syntax error");
    
    Ok(())
}

// Test error handling - Semantic errors
#[test]
fn test_invalid_predicate() -> Result<()> {
    // Test query with invalid predicate (comparing different types)
    let sql = "SELECT id FROM test_table WHERE id = 'string'";
    let mut parser = Parser::new(sql);
    
    // Parsing should succeed (it's syntactically valid)
    let statement = parser.parse_statement().map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
    if let Statement::Select(select) = statement {
        // We just verify we can parse it - actual type checking would happen later
        assert!(select.where_clause.is_some(), "Expected WHERE clause");
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
} 