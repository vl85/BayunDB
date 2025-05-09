use anyhow::{Result, anyhow};
use bayundb::query::parser::parse;
use bayundb::query::parser::ast::{Statement, JoinType};
use bayundb::query::planner::logical::{self, LogicalPlan};
use bayundb::query::planner::physical::{self, PhysicalPlan};
use bayundb::query::executor::result::DataValue;
use bayundb::query::executor::operators::create_table_scan;
use bayundb::query::executor::operators::join::create_hash_join;

#[test]
fn test_join_query() -> Result<()> {
    // Test a query with JOIN operation
    let sql = "SELECT u.id, u.name, o.order_id FROM users u JOIN orders o ON u.id = o.user_id";
    
    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
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
        
        // Verify logical plan has Join node
        match &logical_plan {
            LogicalPlan::Projection { input, .. } => {
                match &**input {
                    LogicalPlan::Join { left, right, join_type, .. } => {
                        // Check join type
                        assert_eq!(*join_type, JoinType::Inner);
                        
                        // Check left side is users table
                        match &**left {
                            LogicalPlan::Scan { table_name, .. } => {
                                assert_eq!(table_name, "users");
                            },
                            _ => panic!("Expected Scan as left input to Join"),
                        }
                        
                        // Check right side is orders table
                        match &**right {
                            LogicalPlan::Scan { table_name, .. } => {
                                assert_eq!(table_name, "orders");
                            },
                            _ => panic!("Expected Scan as right input to Join"),
                        }
                    },
                    _ => panic!("Expected Join under Projection"),
                }
            },
            _ => panic!("Expected Projection as root of logical plan"),
        }
        
        // Create physical plan and verify join selection
        let physical_plan = physical::create_physical_plan(&logical_plan);
        
        // Verify the physical plan has a Hash Join for equality condition
        match &physical_plan {
            PhysicalPlan::Project { input, .. } => {
                match &**input {
                    PhysicalPlan::HashJoin { join_type, .. } => {
                        // Hash join should be selected for equality conditions
                        assert_eq!(*join_type, JoinType::Inner);
                    },
                    _ => panic!("Expected HashJoin under Project for equality join condition"),
                }
            },
            _ => panic!("Expected Project as root of physical plan"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

#[test]
fn test_left_join_query() -> Result<()> {
    // Test a query with LEFT JOIN operation
    let sql = "SELECT u.id, u.name, o.order_id FROM users u LEFT JOIN orders o ON u.id = o.user_id";
    
    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
    // Verify JOIN structure in the AST
    if let Statement::Select(select) = statement {
        // Check join type
        assert_eq!(select.joins.len(), 1, "Should have one JOIN clause");
        let join = &select.joins[0];
        assert_eq!(join.join_type, JoinType::LeftOuter, "Should be a LEFT OUTER JOIN");
        
        // Create logical plan
        let logical_plan = logical::build_logical_plan(&select);
        
        // Verify logical plan has Join node with LEFT join type
        match &logical_plan {
            LogicalPlan::Projection { input, .. } => {
                match &**input {
                    LogicalPlan::Join { left, right, join_type, .. } => {
                        // Check join type
                        assert_eq!(*join_type, JoinType::LeftOuter);
                        
                        // Check left side is users table
                        match &**left {
                            LogicalPlan::Scan { table_name, .. } => {
                                assert_eq!(table_name, "users");
                            },
                            _ => panic!("Expected Scan as left input to Join"),
                        }
                        
                        // Check right side is orders table
                        match &**right {
                            LogicalPlan::Scan { table_name, .. } => {
                                assert_eq!(table_name, "orders");
                            },
                            _ => panic!("Expected Scan as right input to Join"),
                        }
                    },
                    _ => panic!("Expected Join under Projection"),
                }
            },
            _ => panic!("Expected Projection as root of logical plan"),
        }
        
        // Create physical plan and verify join selection
        let physical_plan = physical::create_physical_plan(&logical_plan);
        
        // Verify the physical plan has a Hash Join for the LEFT JOIN
        match &physical_plan {
            PhysicalPlan::Project { input, .. } => {
                match &**input {
                    PhysicalPlan::HashJoin { join_type, .. } => {
                        // Hash join should be selected for equality conditions
                        assert_eq!(*join_type, JoinType::LeftOuter);
                    },
                    _ => panic!("Expected HashJoin under Project for LEFT JOIN with equality condition"),
                }
            },
            _ => panic!("Expected Project as root of physical plan"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

#[test]
fn test_nested_loop_join_selection() -> Result<()> {
    // Test a join query with non-equality condition that should use nested loop join
    let sql = "SELECT u.id, u.name, o.order_id FROM users u JOIN orders o ON u.id > o.user_id";
    
    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
    if let Statement::Select(select) = statement {
        // Create logical plan
        let logical_plan = logical::build_logical_plan(&select);
        
        // Create physical plan
        let physical_plan = physical::create_physical_plan(&logical_plan);
        
        // Verify the physical plan has a Nested Loop Join for non-equality condition
        match &physical_plan {
            PhysicalPlan::Project { input, .. } => {
                match &**input {
                    PhysicalPlan::NestedLoopJoin { join_type, .. } => {
                        // Nested loop join should be selected for non-equality conditions
                        assert_eq!(*join_type, JoinType::Inner);
                    },
                    _ => panic!("Expected NestedLoopJoin under Project for non-equality join condition"),
                }
            },
            _ => panic!("Expected Project as root of physical plan"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

#[test]
fn test_multi_join_query() -> Result<()> {
    // Test a query with multiple JOIN operations
    let sql = "SELECT u.id, u.name, o.order_id, p.product_name 
               FROM users u 
               JOIN orders o ON u.id = o.user_id 
               JOIN products p ON o.product_id = p.id";
    
    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
    if let Statement::Select(select) = statement {
        // Verify we have two JOIN clauses
        assert_eq!(select.joins.len(), 2, "Should have two JOIN clauses");
        
        // Create logical plan
        let logical_plan = logical::build_logical_plan(&select);
        
        // Verify logical plan has multiple Join nodes
        match &logical_plan {
            LogicalPlan::Projection { input, .. } => {
                // First join should be between the result of users-orders join and products
                match &**input {
                    LogicalPlan::Join { left, right, .. } => {
                        // Right should be products table
                        match &**right {
                            LogicalPlan::Scan { table_name, .. } => {
                                assert_eq!(table_name, "products");
                            },
                            _ => panic!("Expected Scan as right input to top-level Join"),
                        }
                        
                        // Left should be another join between users and orders
                        match &**left {
                            LogicalPlan::Join { left: users, right: orders, .. } => {
                                // Check users table
                                match &**users {
                                    LogicalPlan::Scan { table_name, .. } => {
                                        assert_eq!(table_name, "users");
                                    },
                                    _ => panic!("Expected Scan of users table"),
                                }
                                
                                // Check orders table
                                match &**orders {
                                    LogicalPlan::Scan { table_name, .. } => {
                                        assert_eq!(table_name, "orders");
                                    },
                                    _ => panic!("Expected Scan of orders table"),
                                }
                            },
                            _ => panic!("Expected Join as left input to top-level Join"),
                        }
                    },
                    _ => panic!("Expected Join under Projection"),
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
fn test_mixed_join_types() -> Result<()> {
    // Test a query with different JOIN types
    let sql = "SELECT u.id, u.name, o.order_id, p.product_name 
               FROM users u 
               LEFT JOIN orders o ON u.id = o.user_id 
               JOIN products p ON o.product_id = p.id";
    
    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
    if let Statement::Select(select) = statement {
        // Verify we have the correct JOIN types
        assert_eq!(select.joins.len(), 2, "Should have two JOIN clauses");
        assert_eq!(select.joins[0].join_type, JoinType::LeftOuter, "First join should be LEFT JOIN");
        assert_eq!(select.joins[1].join_type, JoinType::Inner, "Second join should be INNER JOIN");
        
        // Create logical plan
        let logical_plan = logical::build_logical_plan(&select);
        
        // Verify the JOIN types are preserved in the logical plan
        match &logical_plan {
            LogicalPlan::Projection { input, .. } => {
                match &**input {
                    LogicalPlan::Join { join_type: top_join_type, .. } => {
                        // Top level should be inner join
                        assert_eq!(*top_join_type, JoinType::Inner);
                        
                        // The nested join should be left outer join
                        // (This is a simplification - in reality the logical plan
                        // optimizer might reorder joins while preserving semantics)
                    },
                    _ => panic!("Expected Join under Projection"),
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
fn test_join_result_rows() -> Result<()> {
    // Test to verify the actual result rows from a JOIN operation
    
    // Create scan operators for both tables
    // Note: Our scan operator only returns data for 'test_table', so we need to use that for both sides
    let left_scan = create_table_scan("test_table")
        .map_err(|e| anyhow!("Failed to create left scan operator: {:?}", e))?;
    
    let right_scan = create_table_scan("test_table") 
        .map_err(|e| anyhow!("Failed to create right scan operator: {:?}", e))?;
    
    // Create a hash join operator with an explicit column = column condition
    // The hash join implementation expects "column = column" format
    let join_op = create_hash_join(
        left_scan, 
        right_scan, 
        "id = id".to_string(), // Simple join condition for testing
        false // Not a left join
    ).map_err(|e| anyhow!("Failed to create join operator: {:?}", e))?;
    
    // Initialize the operator
    {
        let mut op = join_op.lock().map_err(|e| anyhow!("Failed to lock operator: {}", e))?;
        op.init().map_err(|e| anyhow!("Failed to initialize operator: {:?}", e))?;
    }
    
    // Collect result rows
    let mut result_rows = Vec::new();
    {
        let mut op = join_op.lock().map_err(|e| anyhow!("Failed to lock operator: {}", e))?;
        
        // Try to get first row to debug
        let first_row = op.next().map_err(|e| anyhow!("Error getting first row: {:?}", e))?;
        if let Some(row) = first_row {
            result_rows.push(row);
        }
        
        // Get remaining rows
        while let Some(row) = op.next().map_err(|e| anyhow!("Error getting next row: {:?}", e))? {
            result_rows.push(row);
        }
        
        op.close().map_err(|e| anyhow!("Failed to close operator: {:?}", e))?;
    }
    
    // Verify we have at least some results
    assert!(!result_rows.is_empty(), "Expected non-empty join result");
    
    // Our mock table generator in TableScanOperator produces:
    // - test_table: rows with id 0-19, name "Record X"
    // So we should have matching rows with the same IDs
    
    // There should be matches since both scans have IDs 0-19
    assert!(result_rows.len() > 0, "Expected matching rows");
    
    // Check that rows have the expected columns
    for row in &result_rows {
        // Check that 'id' column exists
        assert!(row.get("id").is_some(), "Expected id column in result");
        
        // Verify id is in the expected range
        if let Some(DataValue::Integer(id)) = row.get("id") {
            assert!(*id >= 0 && *id < 20, "Expected id in range [0,19], got {}", id);
        } else {
            panic!("Expected INTEGER id column");
        }
        
        // 'name' column should exist as well
        assert!(row.get("name").is_some(), "Expected name column in result");
    }
    
    Ok(())
} 