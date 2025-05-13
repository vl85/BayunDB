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

// Helper function to create dependencies for tests in this file
fn setup_planner_dependencies() -> (Arc<RwLock<Catalog>>, Arc<BufferPoolManager>) {
    let temp_db_file = tempfile::NamedTempFile::new()
        .expect("Failed to create temp db file for BPM in join_test");
    let buffer_pool = Arc::new(
        BufferPoolManager::new(100, temp_db_file.path())
            .expect("Failed to create BPM for join_test"),
    );
    let catalog = Arc::new(RwLock::new(Catalog::new()));
    (catalog, buffer_pool)
}

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
    page_manager.init_page(&mut page_guard);

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
    let (catalog, buffer_pool) = setup_planner_dependencies(); // Use helper

    // Manually create dummy tables for planning to succeed
    {
        let cat_guard = catalog.write().unwrap();
        let users_columns = vec![
            Column::new("id".to_string(), DataType::Integer, false, true, None),
            Column::new("name".to_string(), DataType::Text, false, false, None),
        ];
        cat_guard.create_table(Table::new("users".to_string(), users_columns)).unwrap();
        let orders_columns = vec![
            Column::new("order_id".to_string(), DataType::Integer, false, true, None),
            Column::new("user_id".to_string(), DataType::Integer, false, false, None),
        ];
        cat_guard.create_table(Table::new("orders".to_string(), orders_columns)).unwrap();
    }
    
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
        let logical_plan = logical::build_logical_plan(&select, catalog.clone());
        
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
        let physical_plan_result = planner::physical::create_physical_plan(&logical_plan, catalog.clone(), buffer_pool.clone());
        assert!(physical_plan_result.is_ok(), "Physical plan creation failed: {:?}", physical_plan_result.err());
        let physical_plan = physical_plan_result.unwrap();
        
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
    let (catalog, buffer_pool) = setup_planner_dependencies(); // Use helper

    // Manually create dummy tables for planning to succeed
    {
        let cat_guard = catalog.write().unwrap();
        // users table
        let users_columns = vec![
            Column::new("id".to_string(), DataType::Integer, false, true, None),
            Column::new("name".to_string(), DataType::Text, false, false, None),
        ];
        let users_table = Table::new("users".to_string(), users_columns);
        cat_guard.create_table(users_table).expect("Failed to create users table for test_left_join_query");

        // orders table
        let orders_columns = vec![
            Column::new("order_id".to_string(), DataType::Integer, false, true, None),
            Column::new("user_id".to_string(), DataType::Integer, false, false, None), // Referenced in join condition
        ];
        let orders_table = Table::new("orders".to_string(), orders_columns);
        cat_guard.create_table(orders_table).expect("Failed to create orders table for test_left_join_query");
    }
    
    // Verify JOIN structure in the AST
    if let Statement::Select(select) = statement {
        // Check join type
        assert_eq!(select.joins.len(), 1, "Should have one JOIN clause");
        let join = &select.joins[0];
        assert_eq!(join.join_type, JoinType::LeftOuter, "Should be a LEFT OUTER JOIN");
        
        // Create logical plan
        let logical_plan = logical::build_logical_plan(&select, catalog.clone());
        
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
        let physical_plan_result = planner::physical::create_physical_plan(&logical_plan, catalog.clone(), buffer_pool.clone());
        assert!(physical_plan_result.is_ok(), "Physical plan creation failed for left join: {:?}", physical_plan_result.err());
        let physical_plan = physical_plan_result.unwrap();
        
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
    let (catalog, buffer_pool) = setup_planner_dependencies(); // Use helper

    // Manually create dummy tables for planning to succeed
    {
        let cat_guard = catalog.write().unwrap();
        // users table
        let users_columns = vec![
            Column::new("id".to_string(), DataType::Integer, false, true, None),
            Column::new("name".to_string(), DataType::Text, false, false, None),
        ];
        let users_table = Table::new("users".to_string(), users_columns);
        cat_guard.create_table(users_table).expect("Failed to create users table for test_nested_loop_join_selection");

        // orders table
        let orders_columns = vec![
            Column::new("order_id".to_string(), DataType::Integer, false, true, None),
            Column::new("user_id".to_string(), DataType::Integer, false, false, None), // Referenced in join condition
        ];
        let orders_table = Table::new("orders".to_string(), orders_columns);
        cat_guard.create_table(orders_table).expect("Failed to create orders table for test_nested_loop_join_selection");
    }

    if let Statement::Select(select) = statement {
        // Create logical plan
        let logical_plan = logical::build_logical_plan(&select, catalog.clone());
        
        // Create physical plan
        let physical_plan_result = planner::physical::create_physical_plan(&logical_plan, catalog.clone(), buffer_pool.clone());
        assert!(physical_plan_result.is_ok(), "Physical plan creation failed for NLJ selection: {:?}", physical_plan_result.err());
        let physical_plan = physical_plan_result.unwrap();
        
        // Verify the physical plan has a Nested Loop Join for non-equality condition
        match &physical_plan {
            PhysicalPlan::Project { input, .. } => {
                match &**input {
                    PhysicalPlan::NestedLoopJoin { join_type, .. } => {
                        // Nested loop join should be selected for non-equality conditions
                        assert_eq!(*join_type, JoinType::Inner);
                    },
                    PhysicalPlan::HashJoin { .. } => {
                        panic!("Expected NestedLoopJoin for non-equality condition, but found HashJoin. Plan: {:?}", physical_plan);
                    }
                    _ => panic!("Expected NestedLoopJoin under Project for non-equality join condition, found {:?}", input),
                }
            },
            _ => panic!("Expected Project as root of physical plan, found {:?}", physical_plan),
        }
    } else {
        panic!("Expected SELECT statement");
    }
    
    Ok(())
}

#[test]
fn test_multi_join_query() -> Result<()> {
    // Test a query with multiple JOIN operations
    let sql = "SELECT c.name, o.order_date, p.product_name 
               FROM customers c 
               JOIN orders o ON c.id = o.customer_id 
               JOIN order_items oi ON o.id = oi.order_id 
               JOIN products p ON oi.product_id = p.id";

    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    let (catalog, buffer_pool) = setup_planner_dependencies();

    // Manually create dummy tables for planning
    {
        let cat_guard = catalog.write().unwrap();
        let cust_cols = vec![Column::new("id".into(), DataType::Integer, false, true, None), Column::new("name".into(), DataType::Text, false, false, None)];
        cat_guard.create_table(Table::new("customers".into(), cust_cols)).unwrap();
        
        let ord_cols = vec![Column::new("id".into(), DataType::Integer, false, true, None), Column::new("customer_id".into(), DataType::Integer, false, false, None), Column::new("order_date".into(), DataType::Text, false, false, None)];
        cat_guard.create_table(Table::new("orders".into(), ord_cols)).unwrap();

        let oi_cols = vec![Column::new("order_id".into(), DataType::Integer, false, false, None), Column::new("product_id".into(), DataType::Integer, false, false, None)];
        cat_guard.create_table(Table::new("order_items".into(), oi_cols)).unwrap();

        let prod_cols = vec![Column::new("id".into(), DataType::Integer, false, true, None), Column::new("product_name".into(), DataType::Text, false, false, None)];
        cat_guard.create_table(Table::new("products".into(), prod_cols)).unwrap();
    }

    if let Statement::Select(select) = statement {
        let logical_plan = logical::build_logical_plan(&select, catalog.clone());

        // Check for nested Join nodes in logical plan
        match &logical_plan {
            LogicalPlan::Projection { input, .. } => {
                match &**input {
                    LogicalPlan::Join { right: p_scan_input, .. } => { // Innermost join (products)
                        match &**p_scan_input {
                            LogicalPlan::Scan { table_name, .. } => assert_eq!(table_name, "products"),
                            _ => panic!("Expected Scan for products table in multi-join logical plan")
                        }
                        // Further checks can be added for the other joins if necessary
                    }
                    _ => panic!("Expected Join as input to Projection in multi-join logical plan")
                }
            }
            _ => panic!("Expected Projection as root of multi-join logical plan")
        }

        let physical_plan_result = planner::physical::create_physical_plan(&logical_plan, catalog.clone(), buffer_pool.clone());
        assert!(physical_plan_result.is_ok(), "Physical plan creation failed for multi-join: {:?}", physical_plan_result.err());
        let physical_plan = physical_plan_result.unwrap();

        // Check for nested HashJoin nodes in physical plan
        match &physical_plan {
            PhysicalPlan::Project { input, .. } => {
                match &**input {
                    PhysicalPlan::HashJoin { right, .. } => { // Innermost join
                        match &**right {
                            PhysicalPlan::SeqScan { table_name, .. } => assert_eq!(table_name, "products"),
                            _ => panic!("Expected SeqScan for products table in multi-join physical plan, got {:?}", right)
                        }
                        // Further checks for other HashJoins can be added
                    }
                    _ => panic!("Expected HashJoin as input to Project in multi-join physical plan, got {:?}", input)
                }
            }
            _ => panic!("Expected Project as root of multi-join physical plan, got {:?}", physical_plan)
        }
    } else {
        panic!("Expected SELECT statement for multi-join test");
    }
    Ok(())
}

#[test]
fn test_mixed_join_types() -> Result<()> {
    // Test a query with a mix of INNER and LEFT JOIN operations
    let sql = "SELECT u.name, o.order_id, oi.item_name 
               FROM users u 
               JOIN orders o ON u.id = o.user_id 
               LEFT JOIN order_items oi ON o.id = oi.order_id";

    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    let (catalog, buffer_pool) = setup_planner_dependencies();

    // Manually create dummy tables for planning
    {
        let cat_guard = catalog.write().unwrap();
        let user_cols = vec![Column::new("id".into(), DataType::Integer, false, true, None), Column::new("name".into(), DataType::Text, false, false, None)];
        cat_guard.create_table(Table::new("users".into(), user_cols)).unwrap();
        
        let ord_cols = vec![Column::new("id".into(), DataType::Integer, false, true, None), Column::new("user_id".into(), DataType::Integer, false, false, None), Column::new("order_id".into(), DataType::Text, false, false, None)];
        cat_guard.create_table(Table::new("orders".into(), ord_cols)).unwrap();

        let oi_cols = vec![Column::new("order_id".into(), DataType::Integer, false, false, None), Column::new("item_name".into(), DataType::Text, false, false, None)];
        cat_guard.create_table(Table::new("order_items".into(), oi_cols)).unwrap();
    }

    if let Statement::Select(select) = statement {
        let logical_plan = logical::build_logical_plan(&select, catalog.clone());

        // Verify logical plan structure for mixed joins
        match &logical_plan {
            LogicalPlan::Projection { input, .. } => {
                match &**input { // Should be the LEFT JOIN
                    LogicalPlan::Join { join_type: left_join_type, left: inner_join_input, right: oi_scan_input, .. } => {
                        assert_eq!(*left_join_type, JoinType::LeftOuter);
                        match &**oi_scan_input {
                            LogicalPlan::Scan { table_name, .. } => assert_eq!(table_name, "order_items"),
                            _ => panic!("Expected Scan for order_items in mixed join logical plan")
                        }
                        match &**inner_join_input { // Should be the INNER JOIN (users JOIN orders)
                            LogicalPlan::Join { join_type: inner_join_type, left: u_scan_input, right: o_scan_input, .. } => {
                                assert_eq!(*inner_join_type, JoinType::Inner);
                                match &**u_scan_input {
                                    LogicalPlan::Scan { table_name, .. } => assert_eq!(table_name, "users"),
                                    _ => panic!("Expected Scan for users in mixed join logical plan")
                                }
                                match &**o_scan_input {
                                    LogicalPlan::Scan { table_name, .. } => assert_eq!(table_name, "orders"),
                                    _ => panic!("Expected Scan for orders in mixed join logical plan")
                                }
                            }
                            _ => panic!("Expected Inner Join as left input to Left Join in mixed join logical plan")
                        }
                    }
                    _ => panic!("Expected Left Join as input to Projection in mixed join logical plan")
                }
            }
            _ => panic!("Expected Projection as root of mixed join logical plan")
        }

        let physical_plan_result = planner::physical::create_physical_plan(&logical_plan, catalog.clone(), buffer_pool.clone());
        assert!(physical_plan_result.is_ok(), "Physical plan creation failed for mixed joins: {:?}", physical_plan_result.err());
        let physical_plan = physical_plan_result.unwrap();

        // Verify physical plan structure (e.g., HashJoins with correct types)
        match &physical_plan {
            PhysicalPlan::Project { input, .. } => {
                match &**input { // Should be the LEFT HashJoin
                    PhysicalPlan::HashJoin { join_type: outer_join_type, left: inner_join_ph_input, right: oi_ph_scan, .. } => {
                        assert_eq!(*outer_join_type, JoinType::LeftOuter);
                         match &**oi_ph_scan {
                            PhysicalPlan::SeqScan { table_name, .. } => assert_eq!(table_name, "order_items"),
                            _ => panic!("Expected SeqScan for order_items in mixed join physical plan, got {:?}", oi_ph_scan)
                        }
                        match &**inner_join_ph_input { // Should be the INNER HashJoin
                            PhysicalPlan::HashJoin { join_type: inner_join_ph_type, .. } => {
                                assert_eq!(*inner_join_ph_type, JoinType::Inner);
                                // Further checks on inputs to inner hash join can be added if needed
                            }
                            _ => panic!("Expected Inner HashJoin as left input to Left HashJoin, got {:?}", inner_join_ph_input)
                        }
                    }
                    _ => panic!("Expected Left HashJoin as input to Project, got {:?}", input)
                }
            }
            _ => panic!("Expected Project as root of mixed join physical plan, got {:?}", physical_plan)
        }

    } else {
        panic!("Expected SELECT statement for mixed-join test");
    }
    Ok(())
}

#[test]
fn test_join_result_rows() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("join_test_results.db");
    let buffer_pool = Arc::new(BufferPoolManager::new(100, &db_path)?);
    let page_manager = PageManager::new();
    let catalog_arc = Arc::new(RwLock::new(Catalog::new()));

    setup_join_test_data(&buffer_pool, &page_manager, &catalog_arc)?;

    // SQL for a simple join query that we can verify results for
    let sql = "SELECT t1.value, t2.data 
               FROM join_test_table t1 
               JOIN join_another_table t2 ON t1.id = t2.fk_id";

    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
    if let Statement::Select(select) = statement {
        let logical_plan = logical::build_logical_plan(&select, catalog_arc.clone());
        
        // Create physical plan
        let physical_plan_result = planner::physical::create_physical_plan(&logical_plan, catalog_arc.clone(), buffer_pool.clone());
        assert!(physical_plan_result.is_ok(), "Physical plan creation failed for join_result_rows: {:?}", physical_plan_result.err());
        let physical_plan = physical_plan_result.unwrap();

        // Minimal check on physical plan structure
        match &physical_plan {
            PhysicalPlan::Project { input, .. } => {
                match &**input {
                    PhysicalPlan::HashJoin { .. } => { /* Expected */ }
                    PhysicalPlan::NestedLoopJoin { .. } => { /* Also possible depending on optimizer/conditions */ }
                    _ => panic!("Expected some join type under Project, got {:?}", input)
                }
            }
            _ => panic!("Expected Project as root of physical plan, got {:?}", physical_plan)
        }

        // The following is existing logic to test operator execution, keeping it for now
        // but noting that full plan execution is out of scope for this specific update.
        let left_scan_op = create_table_scan(
            "join_test_table",
            "t1",
            buffer_pool.clone(),
            catalog_arc.clone(),
        )?;

        let right_scan_op = create_table_scan(
            "join_another_table",
            "t2",
            buffer_pool.clone(),
            catalog_arc.clone(),
        )?;

        let condition_str = "t1.id = t2.fk_id".to_string(); // Manually format the condition string

        let join_op_arc = create_nested_loop_join(
            left_scan_op, 
            right_scan_op, 
            condition_str, // Use manually formatted string
            false, // is_left_join (for InnerJoin)
            "t1".to_string(), 
            "t2".to_string()
        )?;

        let mut op_guard = join_op_arc.lock().unwrap();
        op_guard.init()?;
        let mut results = Vec::new();
        while let Some(row_result) = op_guard.next()? {
            results.push(row_result);
        }
        println!("Join results (manual execution part): {:?}", results);
        assert_eq!(results.len(), 3, "Expected 3 rows from the join based on test data");

        // Verify some data points - depends on the column order from combined schemas
        // Schema of join_test_table: id (INT), value (TEXT)
        // Schema of join_another_table: id (INT), fk_id (INT), data (TEXT)
        // Combined (NLJ default): t1.id, t1.value, t2.id, t2.fk_id, t2.data
        // Selected by SQL: t1.value, t2.data
        // The manual join_op above will output all columns from both tables.
        // The `results` will contain Vec<DataValue> where each DataValue needs to be mapped correctly.
        
        // Example check (assuming the order t1.value, t2.data is NOT what join_op produces by default)
        // We need to be careful here as the manual join op schema is different from projected SQL schema.
        // Let's check for presence of expected pairs if projection is not mimicked perfectly by manual op.

        let expected_pairs = [
            (DataValue::Text("TestA".to_string()), DataValue::Text("DataOne".to_string())),
            (DataValue::Text("TestA".to_string()), DataValue::Text("DataThree".to_string())),
            (DataValue::Text("TestB".to_string()), DataValue::Text("DataTwo".to_string())),
        ];

        for expected_pair in expected_pairs.iter() {
            let found = results.iter().any(|row: &bayundb::query::executor::result::Row| {
                // Assuming t1.value is at index 1 (after t1.id) and t2.data is at index 4 (after t2.id, t2.fk_id)
                // This indexing is fragile and depends on the exact schemas and join implementation details.
                // For a robust test, one might want to map column names to indices via schema.
                // Using qualified names as per operator output schema construction
                row.get("t1.value") == Some(&expected_pair.0) && row.get("t2.data") == Some(&expected_pair.1)
            });
            assert!(found, "Expected pair ({:?}, {:?}) not found in join results", expected_pair.0, expected_pair.1);
        }

    } else {
        panic!("Expected SELECT statement for join_result_rows test");
    }

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