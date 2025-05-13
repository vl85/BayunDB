use anyhow::{Result, anyhow};
use bayundb::query::parser::parse;
use bayundb::query::parser::ast::Statement;
use bayundb::query::planner::logical::{self, LogicalPlan};
use bayundb::query::planner::{self as planner_mod, PhysicalPlan};
use bayundb::query::planner::physical_optimizer::PhysicalOptimizer;
use bayundb::catalog::Catalog;
use std::sync::{Arc, RwLock};
use bayundb::storage::buffer::BufferPoolManager;
use bayundb::catalog::{Table, Column, DataType};
use bayundb::query::planner::Planner;

// Helper function to create dependencies for tests in this file
fn setup_test_env() -> (Planner, Arc<RwLock<Catalog>>, Arc<BufferPoolManager>) {
    let temp_db_file = tempfile::NamedTempFile::new()
        .expect("Failed to create temp db file for BPM in planner_test");
    let buffer_pool = Arc::new(
        BufferPoolManager::new(100, temp_db_file.path())
            .expect("Failed to create BPM for planner_test"),
    );
    let catalog = Arc::new(RwLock::new(Catalog::new()));
    let planner = Planner::new(buffer_pool.clone(), catalog.clone());
    (planner, catalog, buffer_pool)
}

#[test]
fn test_logical_plan_generation() -> Result<()> {
    let sql = "SELECT id, name FROM test_table WHERE id > 5";
    let (_planner, catalog, _buffer_pool) = setup_test_env();
    
    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
    if let Statement::Select(select) = statement {
        let logical_plan = logical::build_logical_plan(&select, catalog.clone());
        match logical_plan {
            LogicalPlan::Projection { columns, input } => {
                assert_eq!(columns, vec!["id".to_string(), "name".to_string()]);
                match *input {
                    LogicalPlan::Filter { input: scan_input, .. } => {
                        match *scan_input {
                            LogicalPlan::Scan { table_name, .. } => {
                                assert_eq!(table_name, "test_table");
                            }
                            _ => panic!("Expected Scan as input to Filter"),
                        }
                    }
                    _ => panic!("Expected Filter as input to Projection"),
                }
            }
            _ => panic!("Expected Projection as root of logical plan"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
    Ok(())
}

#[test]
fn test_physical_plan_generation() -> Result<()> {
    let sql = "SELECT id, name FROM test_table WHERE id > 5";
    let (_planner, catalog, buffer_pool) = setup_test_env();
    
    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    
    if let Statement::Select(select) = statement {
        let logical_plan = logical::build_logical_plan(&select, catalog.clone());
        
        let physical_plan = planner_mod::physical::create_physical_plan(&logical_plan, catalog.clone(), buffer_pool.clone()).unwrap();
        
        let physical_plan_for_opt = physical_plan.clone();
        
        match physical_plan {
            PhysicalPlan::Project { columns, input } => {
                assert_eq!(columns, vec!["id".to_string(), "name".to_string()]);
                match *input {
                    PhysicalPlan::Filter { input: scan_input, .. } => {
                        match *scan_input {
                            PhysicalPlan::SeqScan { table_name, .. } => {
                                assert_eq!(table_name, "test_table");
                            }
                            _ => panic!("Expected SeqScan as input to Filter"),
                        }
                    }
                    _ => panic!("Expected Filter as input to Project"),
                }
            }
            _ => panic!("Expected Project as root of physical plan"),
        }
        
        let optimizer = PhysicalOptimizer::new(catalog.clone());
        let optimized_plan = optimizer.optimize(physical_plan_for_opt);
        assert!(format!("{:?}", optimized_plan).contains("Project"));
    } else {
        panic!("Expected SELECT statement");
    }
    Ok(())
}

#[test]
fn test_wildcard_projection() -> Result<()> {
    let sql = "SELECT * FROM employees";
    let (_planner, catalog, _buffer_pool) = setup_test_env();

    {
        let cat_guard = catalog.write().unwrap();
        let emp_columns = vec![
            Column::new("id".to_string(), DataType::Integer, false, true, None),
            Column::new("name".to_string(), DataType::Text, false, false, None),
            Column::new("role".to_string(), DataType::Text, false, false, None),
        ];
        let employees_table = Table::new("employees".to_string(), emp_columns);
        cat_guard.create_table(employees_table).expect("Failed to create employees table for test");
    }
    
    let statement = parse(sql).map_err(|e| anyhow!("Parse error: {:?}", e))?;
    if let Statement::Select(select) = statement {
        assert_eq!(select.columns.len(), 1);
        let logical_plan = logical::build_logical_plan(&select, catalog.clone());
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

#[test]
fn test_simple_plan_creation() {
    let sql = "SELECT id FROM test_table";
    let (planner, catalog, buffer_pool) = setup_test_env();
    let cat_w = catalog.write().unwrap();
    cat_w.create_table(Table::new(
        "test_table".into(),
        vec![Column::new("id".into(), DataType::Integer, false, true, None)],
    )).unwrap();
    drop(cat_w);

    let stmt = parse(sql).unwrap();
    let logical_plan = planner.create_logical_plan(&stmt).unwrap();

    let physical_plan_direct_result = planner_mod::physical::create_physical_plan(
        &logical_plan,
        catalog.clone(),
        buffer_pool.clone(),
    );
    assert!(
        physical_plan_direct_result.is_ok(),
        "Direct physical plan creation failed: {:?}",
        physical_plan_direct_result.err()
    );
    let _ = physical_plan_direct_result.unwrap();

    let physical_plan_from_planner = planner.create_physical_plan(&logical_plan).unwrap();
    match physical_plan_from_planner {
        PhysicalPlan::Project { input, .. } => match *input {
            PhysicalPlan::SeqScan { table_name, .. } => {
                assert_eq!(table_name, "test_table");
            }
            _ => panic!("Expected SeqScan under Project, got {:?}", input),
        },
        _ => panic!("Expected Project plan, got {:?}", physical_plan_from_planner),
    }
}

#[test]
fn test_projection_precedes_selection() {
    let sql = "SELECT name FROM users WHERE id = 1";
    let (planner, catalog, _buffer_pool) = setup_test_env();
    let cat_w = catalog.write().unwrap();
    cat_w.create_table(Table::new(
        "users".into(),
        vec![
            Column::new("id".into(), DataType::Integer, false, true, None),
            Column::new("name".into(), DataType::Text, false, false, None),
        ],
    )).unwrap();
    drop(cat_w);

    let stmt = parse(sql).unwrap();
    let logical_plan = planner.create_logical_plan(&stmt).unwrap();
    let optimized_plan = planner.create_physical_plan(&logical_plan).unwrap();

    match optimized_plan {
        PhysicalPlan::Project { input, .. } => match *input {
            PhysicalPlan::Filter { .. } => (),
            _ => panic!("Selection should precede projection after optimization, found {:?} under Project", input),
        },
        _ => panic!("Expected Project at the root after optimization, found {:?}", optimized_plan),
    }
}

#[test]
fn test_filter_pushdown_across_join() {
    let (planner, catalog, _buffer_pool) = setup_test_env();
    let cat_w = catalog.write().unwrap();
    cat_w.create_table(Table::new(
        "t1".into(),
        vec![
            Column::new("id".into(), DataType::Integer, false, true, None),
            Column::new("val".into(), DataType::Integer, false, false, None),
        ],
    )).unwrap();
    cat_w.create_table(Table::new(
        "t2".into(),
        vec![
            Column::new("id".into(), DataType::Integer, false, true, None),
            Column::new("t1_id".into(), DataType::Integer, false, false, None),
            Column::new("info".into(), DataType::Text, false, false, None),
        ],
    )).unwrap();
    drop(cat_w);

    let sql = "SELECT t1.val, t2.info FROM t1 JOIN t2 ON t1.id = t2.t1_id WHERE t1.val > 100 AND t2.info = 'test'";
    
    let stmt = parse(sql).unwrap();
    let logical_plan = planner.create_logical_plan(&stmt).unwrap();
    let optimized_plan = planner.create_physical_plan(&logical_plan).unwrap();

    fn find_filter_node(plan: &PhysicalPlan) -> bool {
        match plan {
            PhysicalPlan::Filter { .. } => true,
            PhysicalPlan::Project { input, .. }
            | PhysicalPlan::HashAggregate { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::Materialize { input, .. } => find_filter_node(input),
            PhysicalPlan::HashJoin { left, right, .. }
            | PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                find_filter_node(left) || find_filter_node(right)
            }
            PhysicalPlan::SeqScan { .. }
            | PhysicalPlan::CreateTable { .. }
            | PhysicalPlan::AlterTable { .. } => false,
        }
    }

    fn find_filter_under_join(plan: &PhysicalPlan) -> bool {
        match plan {
            PhysicalPlan::HashJoin { left, right, .. }
            | PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                find_filter_node(left)
                    || find_filter_node(right)
                    || find_filter_under_join(left)
                    || find_filter_under_join(right)
            }
            PhysicalPlan::Project { input, .. }
            | PhysicalPlan::HashAggregate { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::Materialize { input, .. } => find_filter_under_join(input),
            PhysicalPlan::Filter { input, .. } => find_filter_under_join(input),
            _ => false,
        }
    }
    assert!(
        find_filter_under_join(&optimized_plan),
        "Filter pushdown expected under a join. Plan: {:?}",
        optimized_plan
    );
} 