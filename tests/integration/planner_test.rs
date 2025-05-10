use anyhow::{Result, anyhow};
use bayundb::query::parser::parse;
use bayundb::query::parser::ast::Statement;
use bayundb::query::planner::logical::{self, LogicalPlan};
use bayundb::query::planner::{self as planner, PhysicalPlan};
use bayundb::query::planner::physical_optimizer::PhysicalOptimizer;
use bayundb::catalog::Catalog;
use std::sync::{Arc, RwLock};

#[test]
fn test_logical_plan_generation() -> Result<()> {
    // Test conversion from AST to logical plan
    let sql = "SELECT id, name FROM test_table WHERE id > 5";
    
    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    let catalog = Arc::new(RwLock::new(Catalog::new()));
    
    if let Statement::Select(select) = statement {
        // Create logical plan from SELECT statement
        let logical_plan = logical::build_logical_plan(&select, catalog.clone());
        
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
    
    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    let catalog = Arc::new(RwLock::new(Catalog::new()));
    
    if let Statement::Select(select) = statement {
        // Create logical plan from SELECT statement
        let logical_plan = logical::build_logical_plan(&select, catalog.clone());
        
        // Convert to physical plan
        let physical_plan = planner::physical::create_physical_plan(&logical_plan);
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
        let optimizer = PhysicalOptimizer::new();
        let optimized_plan = optimizer.optimize(physical_plan_for_opt);
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
    
    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    let catalog = Arc::new(RwLock::new(Catalog::new()));

    // To test wildcard expansion, we need to add the 'employees' table to the catalog
    {
        let mut cat_guard = catalog.write().unwrap();
        // Define some columns for the employees table
        let emp_columns = vec![
            bayundb::catalog::Column::new("id".to_string(), bayundb::catalog::DataType::Integer, false, true, None),
            bayundb::catalog::Column::new("name".to_string(), bayundb::catalog::DataType::Text, false, false, None),
            bayundb::catalog::Column::new("role".to_string(), bayundb::catalog::DataType::Text, false, false, None),
        ];
        let employees_table = bayundb::catalog::Table::new("employees".to_string(), emp_columns);
        cat_guard.create_table(employees_table).expect("Failed to create employees table for test");
    }
    
    if let Statement::Select(select) = statement {
        // Check we have wildcard column
        assert_eq!(select.columns.len(), 1);
        
        // Create logical plan
        let logical_plan = logical::build_logical_plan(&select, catalog.clone());
        
        // Verify projection includes expanded columns, not wildcard
        if let LogicalPlan::Projection { columns, .. } = logical_plan {
            assert_eq!(columns, vec!["id".to_string(), "name".to_string(), "role".to_string()]);
        } else {
            panic!("Expected Projection as root of logical plan");
        }
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
} 