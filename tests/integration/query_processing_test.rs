use anyhow::Result;
use bayundb::query::parser::Parser;
use bayundb::query::parser::ast::{Statement, Expression, Operator, JoinType};
use bayundb::query::planner::logical::{self, LogicalPlan};
use bayundb::query::planner::physical::{self, PhysicalPlan};

#[test]
fn test_simple_select_query() -> Result<()> {
    // Test basic parser functionality
    let sql = "SELECT id, name FROM test_table WHERE id > 5";
    let mut parser = Parser::new(sql);
    
    let statement = parser.parse_statement().map_err(|e| anyhow::anyhow!("Parse error: {:?}", e))?;
    
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
    
    let statement = parser.parse_statement().map_err(|e| anyhow::anyhow!("Parse error: {:?}", e))?;
    
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

#[test]
fn test_logical_plan_generation() -> Result<()> {
    // Test conversion from AST to logical plan
    let sql = "SELECT id, name FROM test_table WHERE id > 5";
    let mut parser = Parser::new(sql);
    
    let statement = parser.parse_statement().map_err(|e| anyhow::anyhow!("Parse error: {:?}", e))?;
    
    if let Statement::Select(select) = statement {
        // Create logical plan from SELECT statement
        let logical_plan = logical::build_logical_plan(&select);
        
        // Verify logical plan structure (should be Projection -> Filter -> Scan)
        match logical_plan {
            LogicalPlan::Projection { columns, input } => {
                // Check that we're projecting the right columns
                assert_eq!(columns, vec!["id".to_string(), "name".to_string()]);
                
                // Check filter below projection
                match *input {
                    LogicalPlan::Filter { input: scan_input, .. } => {
                        // Check scan below filter
                        match *scan_input {
                            LogicalPlan::Scan { table_name, .. } => {
                                assert_eq!(table_name, "test_table");
                            },
                            _ => panic!("Expected Scan as input to Filter"),
                        }
                    },
                    _ => panic!("Expected Filter as input to Projection"),
                }
            },
            _ => panic!("Expected Projection as root of logical plan"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

#[test]
fn test_physical_plan_generation() -> Result<()> {
    // Test conversion from logical plan to physical plan
    let sql = "SELECT id, name FROM test_table WHERE id > 5";
    let mut parser = Parser::new(sql);
    
    let statement = parser.parse_statement().map_err(|e| anyhow::anyhow!("Parse error: {:?}", e))?;
    
    if let Statement::Select(select) = statement {
        // Create logical plan from SELECT statement
        let logical_plan = logical::build_logical_plan(&select);
        
        // Convert to physical plan
        let physical_plan = physical::create_physical_plan(&logical_plan);
        let physical_plan_for_opt = physical_plan.clone();
        
        // Verify physical plan structure (should be Project -> Filter -> SeqScan)
        match physical_plan {
            PhysicalPlan::Project { columns, input } => {
                // Check that we're projecting the right columns
                assert_eq!(columns, vec!["id".to_string(), "name".to_string()]);
                
                // Check filter below projection
                match *input {
                    PhysicalPlan::Filter { input: scan_input, .. } => {
                        // Check scan below filter
                        match *scan_input {
                            PhysicalPlan::SeqScan { table_name, .. } => {
                                assert_eq!(table_name, "test_table");
                            },
                            _ => panic!("Expected SeqScan as input to Filter"),
                        }
                    },
                    _ => panic!("Expected Filter as input to Project"),
                }
            },
            _ => panic!("Expected Project as root of physical plan"),
        }
        
        // Test adding materialization hints
        let optimized_plan = physical::add_materialization(physical_plan_for_opt);
        assert!(format!("{:?}", optimized_plan).contains("Project"));
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

// Test different query patterns
#[test]
fn test_wildcard_projection() -> Result<()> {
    // Test query with wildcard projection
    let sql = "SELECT * FROM employees";
    let mut parser = Parser::new(sql);
    
    let statement = parser.parse_statement().map_err(|e| anyhow::anyhow!("Parse error: {:?}", e))?;
    
    if let Statement::Select(select) = statement {
        // Check we have wildcard column
        assert_eq!(select.columns.len(), 1);
        
        // Create logical plan
        let logical_plan = logical::build_logical_plan(&select);
        
        // Verify projection includes wildcard
        if let LogicalPlan::Projection { columns, .. } = logical_plan {
            assert!(columns.contains(&"*".to_string()));
        } else {
            panic!("Expected Projection as root of logical plan");
        }
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
    let statement = parser.parse_statement().map_err(|e| anyhow::anyhow!("Parse error: {:?}", e))?;
    
    if let Statement::Select(select) = statement {
        // Create logical plan
        let logical_plan = logical::build_logical_plan(&select);
        
        // Create physical plan
        let physical_plan = physical::create_physical_plan(&logical_plan);
        
        // Building executable operator would likely fail in a real system
        // but we can't test that directly here without more infrastructure
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

// Test JOIN operation parsing and planning
#[test]
fn test_join_query() -> Result<()> {
    // Test a query with JOIN operation
    let sql = "SELECT u.id, u.name, o.order_id FROM users u JOIN orders o ON u.id = o.user_id";
    let mut parser = Parser::new(sql);
    
    let statement = parser.parse_statement().map_err(|e| anyhow::anyhow!("Parse error: {:?}", e))?;
    
    // Verify JOIN structure in the AST
    if let Statement::Select(select) = statement {
        // Check basic structure
        assert_eq!(select.columns.len(), 3);
        assert_eq!(select.from.len(), 1);
        assert_eq!(select.from[0].name, "users");
        assert_eq!(select.from[0].alias, Some("u".to_string()));
        
        // Verify JOIN clause
        assert_eq!(select.joins.len(), 1, "Should have one JOIN clause");
        let join = &select.joins[0];
        assert_eq!(join.join_type, JoinType::Inner, "Should be an INNER JOIN");
        assert_eq!(join.table.name, "orders", "Joined table should be 'orders'");
        assert_eq!(join.table.alias, Some("o".to_string()), "Joined table alias should be 'o'");
        
        // Create logical plan
        let logical_plan = logical::build_logical_plan(&select);
        
        // In a real implementation, JOIN would be represented in the logical plan
        // For now, just verify plan contains the base table
        match &logical_plan {
            LogicalPlan::Projection { input, .. } => {
                // Some simplified validation since JOIN planning might not be fully implemented
                assert!(format!("{:?}", input).contains("users"), "Plan should include users table");
            },
            _ => panic!("Expected Projection as root of logical plan"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

// Test LEFT JOIN operation
#[test]
fn test_left_join_query() -> Result<()> {
    // Test a query with LEFT JOIN operation
    let sql = "SELECT u.id, u.name, o.order_id FROM users u LEFT JOIN orders o ON u.id = o.user_id";
    let mut parser = Parser::new(sql);
    
    let statement = parser.parse_statement().map_err(|e| anyhow::anyhow!("Parse error: {:?}", e))?;
    
    // Verify JOIN structure in the AST
    if let Statement::Select(select) = statement {
        // Check join type
        assert_eq!(select.joins.len(), 1, "Should have one JOIN clause");
        let join = &select.joins[0];
        assert_eq!(join.join_type, JoinType::LeftOuter, "Should be a LEFT OUTER JOIN");
        
        // Create logical plan
        let logical_plan = logical::build_logical_plan(&select);
        
        // Simple verification until JOIN planning is fully implemented
        assert!(format!("{:?}", logical_plan).contains("users"), "Plan should include users table");
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

// More comprehensive tests will be added as the query execution pipeline is implemented
// See docs/query_implementation.md for the planned implementation 