use anyhow::{Result, anyhow};
use bayundb::query::parser::Parser;
use bayundb::query::parser::ast::Statement;
use bayundb::query::planner::logical::{self, LogicalPlan};
use bayundb::query::planner::physical::{self, PhysicalPlan};

#[test]
fn test_logical_plan_generation() -> Result<()> {
    // Test conversion from AST to logical plan
    let sql = "SELECT id, name FROM test_table WHERE id > 5";
    let mut parser = Parser::new(sql);
    
    let statement = parser.parse_statement().map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
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
    
    let statement = parser.parse_statement().map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
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

#[test]
fn test_wildcard_projection() -> Result<()> {
    // Test query with wildcard projection
    let sql = "SELECT * FROM employees";
    let mut parser = Parser::new(sql);
    
    let statement = parser.parse_statement().map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
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