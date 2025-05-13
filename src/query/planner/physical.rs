// Physical Query Plan Conversion
//
// This module handles conversion from logical plans to physical plans.

use crate::query::planner::logical::LogicalPlan;
use crate::query::planner::physical_plan::{PhysicalPlan, PhysicalSelectExpression};
use crate::query::parser::ast::Operator as AstOperator;
use crate::query::parser::ast::Expression;
use std::sync::{Arc, RwLock};
use crate::catalog::Catalog;
use crate::storage::buffer::BufferPoolManager;
use crate::query::planner::QueryResult;

// Helper to check if the join condition is a simple equality
fn is_equi_join_condition(condition: &Expression) -> bool {
    match condition {
        Expression::BinaryOp { op, .. } => matches!(op, AstOperator::Equals),
        _ => false, // Not a binary operation, or not an equality
    }
}

/// Convert a logical plan to a physical plan
pub fn create_physical_plan(logical_plan: &LogicalPlan, catalog: Arc<RwLock<Catalog>>, buffer_pool: Arc<BufferPoolManager>) -> QueryResult<PhysicalPlan> {
    match logical_plan {
        LogicalPlan::Scan { table_name, alias } => {
            Ok(PhysicalPlan::SeqScan {
                table_name: table_name.clone(),
                alias: alias.clone(),
            })
        }
        
        LogicalPlan::Filter { input, predicate } => {
            let physical_input = create_physical_plan(input, catalog, buffer_pool)?;
            Ok(PhysicalPlan::Filter {
                input: Box::new(physical_input),
                predicate: predicate.clone(),
            })
        }
        
        LogicalPlan::Projection { columns, input } => {
            let physical_input = create_physical_plan(input, catalog, buffer_pool)?;
            Ok(PhysicalPlan::Project {
                columns: columns.clone(),
                input: Box::new(physical_input),
            })
        }
        
        LogicalPlan::Join { left, right, condition, join_type } => {
            if is_equi_join_condition(condition) {
                let physical_left = create_physical_plan(left, Arc::clone(&catalog), Arc::clone(&buffer_pool))?;
                let physical_right = create_physical_plan(right, catalog, buffer_pool)?;
                Ok(PhysicalPlan::HashJoin {
                    left: Box::new(physical_left),
                    right: Box::new(physical_right),
                    condition: condition.clone(),
                    join_type: join_type.clone(),
                })
            } else {
                let physical_left = create_physical_plan(left, Arc::clone(&catalog), Arc::clone(&buffer_pool))?;
                let physical_right = create_physical_plan(right, catalog, buffer_pool)?;
                Ok(PhysicalPlan::NestedLoopJoin {
                    left: Box::new(physical_left),
                    right: Box::new(physical_right),
                    condition: condition.clone(),
                    join_type: join_type.clone(),
                })
            }
        }
        
        LogicalPlan::Aggregate { input, group_by, aggregate_functions, select_list, having } => {
            let physical_input = create_physical_plan(input, catalog, buffer_pool)?;
            
            // Convert the logical select list (Expression, OutputName) to PhysicalSelectExpression
            // This list represents the final desired output structure.
            let physical_output_select_list: Vec<PhysicalSelectExpression> = select_list
                .iter()
                .map(|(expr, output_name)| PhysicalSelectExpression {
                    expression: expr.clone(),
                    output_name: output_name.clone(),
                })
                .collect();

            // Determine the aggregates needed for computation (includes those in HAVING and SELECT)
            // This logic might need refinement, but for now, let's assume `physical_output_select_list` contains
            // all necessary aggregates implicitly or explicitly. We will pass this to the physical plan.
            // A more sophisticated approach would analyze `having` and `select_list` separately 
            // to build the minimal set of aggregates to compute.
            // For now, let's use the physical_output_select_list directly for aggregate_select_expressions too,
            // assuming it covers everything needed. This might compute more than necessary if HAVING uses
            // aggregates not in the final SELECT list.
            let physical_aggregate_expressions_for_computation = physical_output_select_list.clone(); // Use output list for now

            Ok(PhysicalPlan::HashAggregate {
                input: Box::new(physical_input),
                group_by: group_by.clone(),
                aggregate_select_expressions: physical_aggregate_expressions_for_computation, // Pass computed aggregates
                having: having.clone(),
                output_select_list: physical_output_select_list, // Pass final output structure
            })
        }
        
        LogicalPlan::CreateTable { table_name, columns } => {
            Ok(PhysicalPlan::CreateTable {
                table_name: table_name.clone(),
                columns: columns.clone(),
            })
        }
        
        LogicalPlan::AlterTable { statement } => {
            Ok(PhysicalPlan::AlterTable {
                statement: statement.clone(),
            })
        }
        LogicalPlan::Sort { input, order_by } => {
            let physical_input = create_physical_plan(input, catalog, buffer_pool)?;
            Ok(PhysicalPlan::Sort {
                input: Box::new(physical_input),
                order_by: order_by.clone(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::planner::logical::LogicalPlan;
    use crate::query::parser::ast::{
        ColumnReference, Value, Operator as AstOperator, JoinType, AggregateFunction, ColumnDef, DataType
    };
    use std::sync::{Arc, RwLock};
    use crate::catalog::Catalog;
    use crate::storage::buffer::BufferPoolManager;

    fn create_test_dependencies() -> (Arc<RwLock<Catalog>>, Arc<BufferPoolManager>) {
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        let temp_db_file = tempfile::NamedTempFile::new().expect("Failed to create temp db file for test BPM");
        let buffer_pool = Arc::new(BufferPoolManager::new(10, temp_db_file.path()).expect("Failed to create test BPM"));
        (catalog, buffer_pool)
    }

    #[test]
    fn test_scan_physical_plan() {
        let logical_plan = LogicalPlan::Scan {
            table_name: "users".to_string(),
            alias: Some("u".to_string()),
        };
        let (catalog, buffer_pool) = create_test_dependencies();
        let physical_plan = create_physical_plan(&logical_plan, catalog, buffer_pool).unwrap();
        match physical_plan {
            PhysicalPlan::SeqScan { table_name, alias } => {
                assert_eq!(table_name, "users");
                assert_eq!(alias, Some("u".to_string()));
            }
            _ => panic!("Expected SeqScan plan"),
        }
    }

    #[test]
    fn test_projection_physical_plan() {
        let logical_plan = LogicalPlan::Projection {
            columns: vec!["id".to_string(), "name".to_string()],
            input: Box::new(LogicalPlan::Scan {
                table_name: "customers".to_string(),
                alias: None,
            }),
        };
        let (catalog, buffer_pool) = create_test_dependencies();
        let physical_plan = create_physical_plan(&logical_plan, catalog, buffer_pool).unwrap();
        match physical_plan {
            PhysicalPlan::Project { columns, input } => {
                assert_eq!(columns, vec!["id".to_string(), "name".to_string()]);
                assert!(matches!(*input, PhysicalPlan::SeqScan { .. }));
            }
            _ => panic!("Expected Project plan"),
        }
    }

    #[test]
    fn test_filter_physical_plan() {
        let logical_plan = LogicalPlan::Filter {
            predicate: Expression::Literal(Value::Boolean(true)),
            input: Box::new(LogicalPlan::Scan {
                table_name: "orders".to_string(),
                alias: Some("o".to_string()),
            }),
        };
        let (catalog, buffer_pool) = create_test_dependencies();
        let physical_plan = create_physical_plan(&logical_plan, catalog, buffer_pool).unwrap();
        // Check the type
        assert!(matches!(physical_plan, PhysicalPlan::Filter { .. }));
    }

    #[test]
    fn test_join_physical_plan_equi() {
        // Equi-join condition
        let condition = Expression::BinaryOp {
            left: Box::new(Expression::Column(ColumnReference { table: Some("a".to_string()), name: "id".to_string() })),
            op: AstOperator::Equals,
            right: Box::new(Expression::Column(ColumnReference { table: Some("b".to_string()), name: "id".to_string() })),
        };
        let logical_plan = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan { table_name: "table_a".to_string(), alias: Some("a".to_string()) }),
            right: Box::new(LogicalPlan::Scan { table_name: "table_b".to_string(), alias: Some("b".to_string()) }),
            condition,
            join_type: JoinType::Inner,
        };
        let (catalog, buffer_pool) = create_test_dependencies();
        let physical_plan = create_physical_plan(&logical_plan, catalog, buffer_pool).unwrap();
        // Check the type (should be a hash join)
        assert!(matches!(physical_plan, PhysicalPlan::HashJoin { .. }));
    }

    #[test]
    fn test_join_physical_plan_non_equi() {
        // Non-equi-join condition
        let condition = Expression::BinaryOp {
            left: Box::new(Expression::Column(ColumnReference { table: Some("a".to_string()), name: "id".to_string() })),
            op: AstOperator::GreaterThan,
            right: Box::new(Expression::Column(ColumnReference { table: Some("b".to_string()), name: "id".to_string() })),
        };
        let logical_plan = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan { table_name: "table_a".to_string(), alias: Some("a".to_string()) }),
            right: Box::new(LogicalPlan::Scan { table_name: "table_b".to_string(), alias: Some("b".to_string()) }),
            condition,
            join_type: JoinType::Inner,
        };
        let (catalog, buffer_pool) = create_test_dependencies();
        let physical_plan = create_physical_plan(&logical_plan, catalog, buffer_pool).unwrap();
        // Check the type (should be a nested loop join)
        assert!(matches!(physical_plan, PhysicalPlan::NestedLoopJoin { .. }));
    }

    #[test]
    fn test_aggregate_physical_plan() {
        let group_expr = Expression::Column(ColumnReference { table: None, name: "category".to_string() });
        let agg_expr = Expression::Aggregate {
            function: AggregateFunction::Count,
            arg: None, // COUNT(*)
        };
        let logical_plan = LogicalPlan::Aggregate {
            group_by: vec![group_expr.clone()],
            aggregate_functions: vec![agg_expr.clone()], // Raw functions found
            select_list: vec![ // Final output structure
                (group_expr.clone(), "category".to_string()),
                (agg_expr.clone(), "total".to_string()),
            ],
            having: None,
            input: Box::new(LogicalPlan::Scan { table_name: "products".to_string(), alias: None }),
        };
        let (catalog, buffer_pool) = create_test_dependencies();
        let physical_plan = create_physical_plan(&logical_plan, catalog, buffer_pool).unwrap();

        // Check the type and fields
        match physical_plan {
            PhysicalPlan::HashAggregate { input, group_by, aggregate_select_expressions, having, output_select_list } => {
                assert!(matches!(*input, PhysicalPlan::SeqScan { .. }));
                assert_eq!(group_by.len(), 1);
                // aggregate_select_expressions now derived from output_select_list in current conversion logic
                assert_eq!(aggregate_select_expressions.len(), 2); 
                assert_eq!(output_select_list.len(), 2);
                assert_eq!(output_select_list[0].output_name, "category");
                assert_eq!(output_select_list[1].output_name, "total");
                assert!(having.is_none());
            }
            _ => panic!("Expected PhysicalPlan::HashAggregate, got {:?}", physical_plan),
        }
    }
    
    #[test]
    fn test_create_table_physical_plan() {
        let logical_plan = LogicalPlan::CreateTable {
            table_name: "users".to_string(),
            columns: vec![ColumnDef { 
                name: "id".to_string(), 
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                default_value: None,
            }],
        };
        let (catalog, buffer_pool) = create_test_dependencies();
        let physical_plan = create_physical_plan(&logical_plan, catalog, buffer_pool).unwrap();
        match physical_plan {
            PhysicalPlan::CreateTable { table_name, columns } => {
                assert_eq!(table_name, "users");
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].name, "id");
                assert!(matches!(columns[0].data_type, DataType::Integer));
                assert_eq!(columns[0].nullable, false);
                assert_eq!(columns[0].primary_key, true);
                assert!(columns[0].default_value.is_none());
            }
            _ => panic!("Expected CreateTable plan"),
        }
    }
    
    #[test]
    fn test_alter_table_physical_plan() {
        let logical_plan = LogicalPlan::AlterTable {
            statement: crate::query::parser::ast::AlterTableStatement {
                table_name: "users".to_string(),
                operation: crate::query::parser::ast::AlterTableOperation::AddColumn(
                    ColumnDef {
                        name: "age".to_string(),
                        data_type: DataType::Integer,
                        nullable: true,
                        primary_key: false,
                        default_value: None,
                    }
                )
            }
        };
        let (catalog, buffer_pool) = create_test_dependencies();
        let physical_plan = create_physical_plan(&logical_plan, catalog, buffer_pool).unwrap();
        match physical_plan {
            PhysicalPlan::AlterTable { statement } => {
                assert_eq!(statement.table_name, "users");
                assert!(matches!(statement.operation, crate::query::parser::ast::AlterTableOperation::AddColumn(_)));
            }
            _ => panic!("Expected AlterTable plan"),
        }
    }
    
    #[test]
    fn test_sort_physical_plan() {
        let logical_plan = LogicalPlan::Sort {
            input: Box::new(LogicalPlan::Scan { 
                table_name: "users".to_string(), 
                alias: None 
            }),
            order_by: vec![(Expression::Column(ColumnReference{table: None, name: "age".to_string()}), true)], // age DESC
        };
        let (catalog, buffer_pool) = create_test_dependencies();
        let physical_plan = create_physical_plan(&logical_plan, catalog, buffer_pool).unwrap();
        match physical_plan {
            PhysicalPlan::Sort { input, order_by } => {
                assert!(matches!(*input, PhysicalPlan::SeqScan { .. }));
                assert_eq!(order_by.len(), 1);
                assert!(order_by[0].1); // Check DESC flag
            }
            _ => panic!("Expected Sort plan, got {:?}", physical_plan),
        }
    }
} 