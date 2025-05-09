use anyhow::{Result, anyhow};
use bayundb::query::parser::Parser;
use bayundb::query::parser::ast::{Statement, AggregateFunction, Expression};

#[test]
fn test_count_query() -> Result<()> {
    // Test a simple COUNT(*) query
    let sql = "SELECT COUNT(*) FROM users";
    let mut parser = Parser::new(sql);
    
    let statement = parser.parse_statement().map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
    // Verify the AST structure
    if let Statement::Select(select) = statement {
        assert_eq!(select.columns.len(), 1);
        
        // Verify we have a COUNT(*) expression
        match &select.columns[0] {
            bayundb::query::parser::ast::SelectColumn::Expression { expr, .. } => {
                if let Expression::Aggregate { function, arg } = &**expr {
                    assert_eq!(*function, AggregateFunction::Count);
                    assert!(arg.is_none(), "Expected COUNT(*) with no argument");
                } else {
                    panic!("Expected aggregate function expression");
                }
            }
            _ => panic!("Expected Expression in column list"),
        }
        
        // Rest of implementation will be added once the logical planner 
        // supports aggregation operators
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

#[test]
fn test_group_by_query() -> Result<()> {
    // Test GROUP BY query
    let sql = "SELECT department_id, COUNT(*) FROM employees GROUP BY department_id";
    let mut parser = Parser::new(sql);
    
    let statement = parser.parse_statement().map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
    // Verify the AST structure
    if let Statement::Select(select) = statement {
        // Check GROUP BY clause
        assert!(select.group_by.is_some(), "Expected GROUP BY clause");
        let group_by = select.group_by.unwrap();
        assert_eq!(group_by.len(), 1, "Expected one column in GROUP BY");
        
        // Verify columns match GROUP BY
        assert_eq!(select.columns.len(), 2, "Expected two columns");
        
        // First column should be department_id
        match &select.columns[0] {
            bayundb::query::parser::ast::SelectColumn::Column(col_ref) => {
                assert_eq!(col_ref.name, "department_id");
            }
            _ => panic!("Expected Column in first position"),
        }
        
        // Second column should be COUNT(*)
        match &select.columns[1] {
            bayundb::query::parser::ast::SelectColumn::Expression { expr, .. } => {
                if let Expression::Aggregate { function, .. } = &**expr {
                    assert_eq!(*function, AggregateFunction::Count);
                } else {
                    panic!("Expected aggregate function expression");
                }
            }
            _ => panic!("Expected Expression in second position"),
        }
        
        // Rest of implementation will be added once the logical planner 
        // supports aggregation operators
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

#[test]
fn test_having_clause() -> Result<()> {
    // Test HAVING clause
    let sql = "SELECT department_id, COUNT(*) FROM employees GROUP BY department_id HAVING COUNT(*) > 5";
    let mut parser = Parser::new(sql);
    
    let statement = parser.parse_statement().map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
    // Verify the AST structure
    if let Statement::Select(select) = statement {
        // Check GROUP BY clause
        assert!(select.group_by.is_some(), "Expected GROUP BY clause");
        
        // Check HAVING clause
        assert!(select.having.is_some(), "Expected HAVING clause");
        
        // Check HAVING contains an aggregate function
        if let Some(having) = &select.having {
            match &**having {
                Expression::BinaryOp { left, .. } => {
                    // Left side should be COUNT(*) 
                    if let Expression::Aggregate { function, .. } = &**left {
                        assert_eq!(*function, AggregateFunction::Count);
                    } else {
                        panic!("Expected aggregate function in HAVING clause");
                    }
                }
                _ => panic!("Expected binary operation in HAVING clause"),
            }
        }
        
        // Rest of implementation will be added once the logical planner 
        // supports aggregation operators
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

#[test]
fn test_multiple_aggregate_functions() -> Result<()> {
    // Test multiple aggregate functions
    let sql = "SELECT department_id, COUNT(*), SUM(salary), AVG(salary), MIN(salary), MAX(salary) \
               FROM employees \
               GROUP BY department_id";
    let mut parser = Parser::new(sql);
    
    let statement = parser.parse_statement().map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
    // Verify the AST structure
    if let Statement::Select(select) = statement {
        // Check columns contain all aggregate functions
        assert_eq!(select.columns.len(), 6, "Expected six columns");
        
        // Check for aggregate functions
        let mut has_count = false;
        let mut has_sum = false;
        let mut has_avg = false;
        let mut has_min = false;
        let mut has_max = false;
        
        for col in &select.columns[1..] { // Skip first column (department_id)
            if let bayundb::query::parser::ast::SelectColumn::Expression { expr, .. } = col {
                if let Expression::Aggregate { function, .. } = &**expr {
                    match function {
                        AggregateFunction::Count => has_count = true,
                        AggregateFunction::Sum => has_sum = true,
                        AggregateFunction::Avg => has_avg = true, 
                        AggregateFunction::Min => has_min = true,
                        AggregateFunction::Max => has_max = true,
                    }
                }
            }
        }
        
        assert!(has_count, "COUNT not found");
        assert!(has_sum, "SUM not found");
        assert!(has_avg, "AVG not found");
        assert!(has_min, "MIN not found");
        assert!(has_max, "MAX not found");
        
        // Rest of implementation will be added once the logical planner 
        // supports aggregation operators
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

#[test]
fn test_complex_aggregation_query() -> Result<()> {
    // Test a complex query with filtering, grouping, and having
    let sql = "SELECT department_id, job_title, COUNT(*), AVG(salary) \
               FROM employees \
               WHERE status = 'active' \
               GROUP BY department_id, job_title \
               HAVING COUNT(*) > 2";
    let mut parser = Parser::new(sql);
    
    let statement = parser.parse_statement().map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
    // Verify the AST structure
    if let Statement::Select(select) = statement {
        // Check basic structure
        assert_eq!(select.columns.len(), 4, "Expected four columns");
        assert!(select.where_clause.is_some(), "Expected WHERE clause");
        assert!(select.group_by.is_some(), "Expected GROUP BY clause");
        assert!(select.having.is_some(), "Expected HAVING clause");
        
        // Check GROUP BY has two columns
        let group_by = select.group_by.unwrap();
        assert_eq!(group_by.len(), 2, "Expected two columns in GROUP BY");
        
        // Check HAVING has a comparison condition
        if let Some(having) = &select.having {
            match &**having {
                Expression::BinaryOp { op, .. } => {
                    assert_eq!(*op, bayundb::query::parser::ast::Operator::GreaterThan);
                }
                _ => panic!("Expected comparison operation in HAVING clause"),
            }
        }
        
        // Rest of implementation will be added once the logical planner 
        // supports aggregation operators
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

// This main function helps with manual testing but is ignored in cargo test runs
#[allow(dead_code)]
fn main() {
    println!("Running aggregation integration tests");
    
    // Run all the tests
    if let Err(e) = test_count_query() {
        eprintln!("test_count_query failed: {}", e);
    } else {
        println!("test_count_query passed");
    }
    
    if let Err(e) = test_group_by_query() {
        eprintln!("test_group_by_query failed: {}", e);
    } else {
        println!("test_group_by_query passed");
    }
    
    if let Err(e) = test_having_clause() {
        eprintln!("test_having_clause failed: {}", e);
    } else {
        println!("test_having_clause passed");
    }
    
    if let Err(e) = test_multiple_aggregate_functions() {
        eprintln!("test_multiple_aggregate_functions failed: {}", e);
    } else {
        println!("test_multiple_aggregate_functions passed");
    }
    
    if let Err(e) = test_complex_aggregation_query() {
        eprintln!("test_complex_aggregation_query failed: {}", e);
    } else {
        println!("test_complex_aggregation_query passed");
    }
    
    println!("All tests completed");
} 