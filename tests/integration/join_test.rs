use anyhow::{Result, anyhow};
use bayundb::query::parser::parse;
use bayundb::query::parser::ast::{Statement, JoinType};
use bayundb::query::planner::logical::{self, LogicalPlan};
use bayundb::query::planner::{self as planner, PhysicalPlan};
use bayundb::query::executor::result::DataValue;
use bayundb::query::executor::operators::create_table_scan;
use std::sync::Arc;
use tempfile::TempDir;
use bayundb::storage::buffer::BufferPoolManager;
use bayundb::query::executor::operators::join::create_nested_loop_join;
use bayundb::catalog::{Catalog, Table, Column, DataType};
use bayundb::storage::page::PageManager;
use std::sync::RwLock;

// Helper function to set up tables and insert data for join tests
fn setup_join_test_data(buffer_pool: &Arc<BufferPoolManager>, page_manager: &PageManager, catalog_arc: &Arc<RwLock<Catalog>>) -> Result<()> {
    let mut catalog_guard = catalog_arc.write().unwrap();

    // Table names
    let test_table_name = "join_test_table";
    let another_table_name = "join_another_table";

    // Attempt to drop tables if they already exist, to ensure a clean state for this test's data
    // Ignore errors if tables don't exist (e.g., first time run)
    let _ = catalog_guard.drop_table_from_current_schema(test_table_name);
    let _ = catalog_guard.drop_table_from_current_schema(another_table_name);

    // Table: join_test_table (id INT PRIMARY KEY, value TEXT)
    let columns1 = vec![
        Column::new("id".to_string(), DataType::Integer, false, true, None),
        Column::new("value".to_string(), DataType::Text, true, false, None),
    ];
    let table1 = Table::new(test_table_name.to_string(), columns1);
    catalog_guard.create_table(table1).map_err(|e| anyhow!(e))?;
    // Insert data into test_table
    let test_table_data: Vec<Vec<DataValue>> = vec![
        vec![DataValue::Integer(100), DataValue::Text("TestA".to_string())],
        vec![DataValue::Integer(101), DataValue::Text("TestB".to_string())],
        vec![DataValue::Integer(102), DataValue::Text("TestC".to_string())], // This one won't match any fk_id in another_table based on current assertions
    ];
    insert_data_into_table(buffer_pool, page_manager, &mut catalog_guard, test_table_name, test_table_data)?;

    // Table: join_another_table (id INT PRIMARY KEY, fk_id INT, data TEXT)
    let columns2 = vec![
        Column::new("id".to_string(), DataType::Integer, false, true, None),
        Column::new("fk_id".to_string(), DataType::Integer, true, false, None),
        Column::new("data".to_string(), DataType::Text, true, false, None),
    ];
    let table2 = Table::new(another_table_name.to_string(), columns2);
    catalog_guard.create_table(table2).map_err(|e| anyhow!(e))?;
    // Insert data into another_table
    let another_table_data: Vec<Vec<DataValue>> = vec![
        vec![DataValue::Integer(200), DataValue::Integer(100), DataValue::Text("DataOne".to_string())],
        vec![DataValue::Integer(201), DataValue::Integer(101), DataValue::Text("DataTwo".to_string())],
        vec![DataValue::Integer(202), DataValue::Integer(100), DataValue::Text("DataThree".to_string())], // Second row matching fk_id = 100
    ];
    insert_data_into_table(buffer_pool, page_manager, &mut catalog_guard, another_table_name, another_table_data)?;

    Ok(())
}

// Helper to insert Vec<Vec<DataValue>> into a specified table
fn insert_data_into_table(
    buffer_pool: &Arc<BufferPoolManager>, 
    page_manager: &PageManager, 
    catalog_guard: &mut std::sync::RwLockWriteGuard<Catalog>, // Pass mutable guard
    table_name: &str, 
    all_rows_data: Vec<Vec<DataValue>>
) -> Result<()> {
    let table = catalog_guard.get_table_mut_from_current_schema(table_name)
        .ok_or_else(|| anyhow!("Table '{}' not found in current schema for data insertion", table_name))?;

    let (page_rc, page_id) = buffer_pool.new_page().map_err(|e| anyhow!("Failed to get new page for {}: {:?}", table_name, e))?;
    table.set_first_page_id(page_id);
    let mut page_guard = page_rc.write();
    page_manager.init_page(&mut *page_guard);

    println!(
        "[INSERT DATA] Table: '{}', PageID: {}, About to insert {} rows. First row preview: {:?}",
        table_name,
        page_id,
        all_rows_data.len(),
        all_rows_data.first()
    );

    for row_data in all_rows_data {
        let record_bytes = bincode::serialize(&row_data)
            .map_err(|e| anyhow!("Failed to serialize record for {}: {}", table_name, e))?;
        page_manager.insert_record(&mut page_guard, &record_bytes)
            .map_err(|e| anyhow!("Failed to insert record into {}: {:?}", table_name, e))?;
    }
    buffer_pool.unpin_page(page_id, true).map_err(|e| anyhow!("Failed to unpin page for {}: {:?}", table_name, e))?;
    Ok(())
}

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
        let physical_plan = planner::physical::create_physical_plan(&logical_plan);
        
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
        let physical_plan = planner::physical::create_physical_plan(&logical_plan);
        
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
        let physical_plan = planner::physical::create_physical_plan(&logical_plan);
        
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
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("join_test_results.db");
    let buffer_pool = Arc::new(BufferPoolManager::new(100, db_path.to_str().unwrap())?);
    let page_manager = PageManager::new();
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));

    setup_join_test_data(&buffer_pool, &page_manager, &catalog_arc)?;

    let t1_alias = "t1";
    let t2_alias = "t2";

    let left_scan_op = create_table_scan("join_test_table", t1_alias, buffer_pool.clone(), catalog_arc.clone())?;
    let right_scan_op = create_table_scan("join_another_table", t2_alias, buffer_pool.clone(), catalog_arc.clone())?;
    
    // Join condition as a string
    let join_condition_str = format!("{}.id = {}.fk_id", t1_alias, t2_alias); // "t1.id = t2.fk_id"

    let join_op_arc = create_nested_loop_join(
        left_scan_op, 
        right_scan_op, 
        join_condition_str, 
        false, // is_left_join (for InnerJoin)
        t1_alias.to_string(), 
        t2_alias.to_string()
    )?;
    
    let mut op_guard = join_op_arc.lock().unwrap();
    op_guard.init()?;
    let mut results = Vec::new();
    while let Some(row) = op_guard.next()? {
        results.push(row);
    }
    op_guard.close()?;

    assert_eq!(results.len(), 3, "Expected 3 rows from the join due to one-to-many match on t1.id=100 and one-to-one on t1.id=101");

    let mut match_count_100 = 0;
    let mut match_count_101 = 0;

    for row in results {
        println!("Joined Row: {:?}", row);
        let t1_id_key = format!("{}.id", t1_alias);
        let t1_value_key = format!("{}.value", t1_alias);
        let t2_fk_id_key = format!("{}.fk_id", t2_alias);
        let t2_data_key = format!("{}.data", t2_alias);

        let t1_id = row.get(&t1_id_key).expect(&format!("Joined row should have {}", t1_id_key));
        let t1_value = row.get(&t1_value_key).expect(&format!("Joined row should have {}", t1_value_key));
        let t2_fk_id = row.get(&t2_fk_id_key).expect(&format!("Joined row should have {}", t2_fk_id_key));
        let t2_data = row.get(&t2_data_key).expect(&format!("Joined row should have {}", t2_data_key));

        assert_eq!(t1_id, t2_fk_id, "Join condition t1.id = t2.fk_id not met in result");

        if let DataValue::Integer(id_val) = t1_id {
            if *id_val == 100 { // Keep deref here if t1_id is &DataValue::Integer
                match_count_100 += 1;
                assert_eq!(t1_value, &DataValue::Text("TestA".to_string()));
                if let DataValue::Text(data_str) = t2_data {
                    assert!(data_str == "DataOne" || data_str == "DataThree");
                } else {
                    panic!("t2.data was not Text for fk_id=100");
                }
            } else if *id_val == 101 {
                match_count_101 += 1;
                assert_eq!(t1_value, &DataValue::Text("TestB".to_string()));
                assert_eq!(t2_data, &DataValue::Text("DataTwo".to_string()));
            } else {
                panic!("Unexpected t1.id in join result: {}", id_val);
            }
        } else {
            panic!("t1.id was not an Integer");
        }
    }
    assert_eq!(match_count_100, 2, "Expected two matches for t1.id = 100");
    assert_eq!(match_count_101, 1, "Expected one match for t1.id = 101");

    Ok(())
}

/// Test nested loop join with different conditions
#[test]
fn test_nested_loop_join_with_condition() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("nested_loop_join_test.db");
    let buffer_pool = Arc::new(BufferPoolManager::new(100, db_path.to_str().unwrap())?);
    let page_manager = PageManager::new();
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));

    setup_join_test_data(&buffer_pool, &page_manager, &catalog_arc)?;

    let left_alias_str = "left_tbl";
    let right_alias_str = "right_tbl";

    let left_scan_op = create_table_scan("join_test_table", left_alias_str, buffer_pool.clone(), catalog_arc.clone())?;
    let right_scan_op = create_table_scan("join_another_table", right_alias_str, buffer_pool.clone(), catalog_arc.clone())?;

    // Join condition: left_tbl.id = right_tbl.fk_id
    let join_condition_str = format!("{}.id = {}.fk_id", left_alias_str, right_alias_str);

    let join_op_arc = create_nested_loop_join(
        left_scan_op, 
        right_scan_op, 
        join_condition_str, 
        false, // is_left_join (for InnerJoin)
        left_alias_str.to_string(), 
        right_alias_str.to_string()
    )?;

    let mut op_guard = join_op_arc.lock().unwrap();
    op_guard.init()?;
    let mut result_rows = Vec::new();
    while let Some(row) = op_guard.next()? {
        result_rows.push(row);
    }
    op_guard.close()?;

    assert!(!result_rows.is_empty(), "Expected non-empty join result");
    assert_eq!(result_rows.len(), 3, "Expected 3 rows from join with condition"); 

    for row in result_rows {
        let left_id_key = format!("{}.id", left_alias_str);
        let right_fk_id_key = format!("{}.fk_id", right_alias_str);
        let left_id = row.get(&left_id_key).expect("Row should have left_table.id");
        let right_fk_id = row.get(&right_fk_id_key).expect("Row should have right_table.fk_id");
        assert_eq!(left_id, right_fk_id, "Join condition failed in results");
    }

    Ok(())
} 