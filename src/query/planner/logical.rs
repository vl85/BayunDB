// Logical Query Plan Implementation
//
// This module defines the logical plan representation for query processing.

use std::fmt;
use std::sync::{Arc, RwLock};
// use crate::catalog::Schema; // Unused import as per cargo build
// crate::common::types::DataType was an erroneous addition and is ensured to be removed.
#[allow(unused_imports)] // For AggregateFunction, which is used in tests but flagged by cargo build.
use crate::query::parser::ast::{Expression, SelectStatement, SelectColumn, ColumnReference, JoinType, ColumnDef, AggregateFunction, AlterTableStatement};
use crate::catalog::{Catalog}; // Removed Table import
// Statement is reported as unused by cargo build.
// use crate::query::parser::ast::Statement;
// use crate::query::parser::ast::AggregateFunction; // This was part of a combined import, now handled by commenting its usage in the line above.

/// Represents a node in the logical query plan
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    /// Scan a table
    Scan {
        /// Table name
        table_name: String,
        /// Table alias (if any)
        alias: Option<String>,
    },
    /// Filter rows based on a predicate
    Filter {
        /// Predicate expression
        predicate: Expression,
        /// Input plan
        input: Box<LogicalPlan>,
    },
    /// Project columns
    Projection {
        /// Columns to project
        columns: Vec<String>,
        /// Input plan
        input: Box<LogicalPlan>,
    },
    /// Join two relations
    Join {
        /// Left input plan
        left: Box<LogicalPlan>,
        /// Right input plan
        right: Box<LogicalPlan>,
        /// Join condition
        condition: Expression,
        /// Join type
        join_type: JoinType,
    },
    /// Aggregation operation (GROUP BY)
    Aggregate {
        /// Input plan
        input: Box<LogicalPlan>,
        /// Group by expressions
        group_by: Vec<Expression>,
        /// Aggregate expressions (COUNT, SUM, etc.)
        aggregate_expressions: Vec<Expression>,
        /// Having clause (optional)
        having: Option<Expression>,
    },
    /// Create a new table
    CreateTable {
        /// Table name
        table_name: String,
        /// Column definitions
        columns: Vec<ColumnDef>,
    },
    /// Alter an existing table
    AlterTable {
        /// The AlterTable statement from the AST
        statement: AlterTableStatement,
    },
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalPlan::Scan { table_name, alias } => {
                if let Some(a) = alias {
                    write!(f, "Scan: {} as {}", table_name, a)
                } else {
                    write!(f, "Scan: {}", table_name)
                }
            }
            LogicalPlan::Filter { predicate, input } => {
                write!(f, "Filter: {:?}\n  {}", predicate, input)
            }
            LogicalPlan::Projection { columns, input } => {
                write!(f, "Projection: {}\n  {}", columns.join(", "), input)
            }
            LogicalPlan::Join { left, right, condition, join_type } => {
                write!(f, "{:?} Join: {:?}\n  Left: {}\n  Right: {}", 
                       join_type, condition, left, right)
            }
            LogicalPlan::Aggregate { input, group_by, aggregate_expressions, having } => {
                let agg_str = aggregate_expressions.iter()
                    .map(|e| format!("{:?}", e))
                    .collect::<Vec<_>>()
                    .join(", ");
                
                let group_by_str = if group_by.is_empty() {
                    "".to_string()
                } else {
                    format!(" GROUP BY [{}]", group_by.iter()
                        .map(|e| format!("{:?}", e))
                        .collect::<Vec<_>>()
                        .join(", "))
                };
                
                let having_str = if let Some(having_expr) = having {
                    format!(" HAVING {:?}", having_expr)
                } else {
                    "".to_string()
                };
                
                write!(f, "Aggregate: [{}]{}{}\n  {}", agg_str, group_by_str, having_str, input)
            }
            LogicalPlan::CreateTable { table_name, columns } => {
                write!(f, "CreateTable: {} with {} columns", table_name, columns.len())
            }
            LogicalPlan::AlterTable { statement } => {
                write!(f, "AlterTable: {}", statement)
            }
        }
    }
}

/// Extract column names from SelectColumn variants, expanding wildcard if catalog is provided.
fn extract_column_names(
    columns: &[SelectColumn],
    source_table_name: Option<&str>, // Name of the table if wildcard needs expansion
    catalog: Option<&Arc<RwLock<Catalog>>>, // Catalog to get schema for wildcard
) -> Vec<String> {
    let mut result = Vec::new();
    let mut expanded_wildcard = false;

    for col in columns {
        match col {
            SelectColumn::Wildcard => {
                if let (Some(table_name), Some(cat)) = (source_table_name, catalog) {
                    if !expanded_wildcard { // Expand wildcard only once even if multiple SelectColumn::Wildcard are present
                        let catalog_guard = cat.read().unwrap(); // Handle error appropriately in real code
                        if let Some(table_schema) = catalog_guard.get_table(table_name) {
                            for column_def in table_schema.columns() {
                                result.push(column_def.name().to_string());
                            }
                            expanded_wildcard = true;
                        } else {
                            // Table not found, fallback or error. For now, push "*"
                            result.push("*".to_string());
                        }
                    } // else, wildcard already expanded, so this SelectColumn::Wildcard adds nothing new
                } else {
                    // No catalog or table name for wildcard expansion, use placeholder
                    result.push("*".to_string());
                }
            }
            SelectColumn::Column(col_ref) => {
                result.push(col_ref.name.clone());
            }
            SelectColumn::Expression { expr, alias } => {
                if let Some(alias_name) = alias {
                    result.push(alias_name.clone());
                } else {
                    // For an expression without an alias, generate a name based on the expression
                    result.push(expr.to_string()); // Use expr.to_string() as a default name
                }
            }
        }
    }
    result
}

/// Extract aggregate expressions from SelectColumn variants
fn extract_aggregate_expressions(columns: &[SelectColumn]) -> Vec<Expression> {
    let mut result = Vec::new();
    
    for col in columns {
        if let SelectColumn::Expression { expr, .. } = col {
            if let Expression::Aggregate { .. } = **expr {
                result.push((**expr).clone());
            }
        }
    }
    
    result
}

/// Checks if a SELECT statement contains any aggregate functions
fn has_aggregates(stmt: &SelectStatement) -> bool {
    // Check in SELECT columns
    for col in &stmt.columns {
        if let SelectColumn::Expression { expr, .. } = col {
            if let Expression::Aggregate { .. } = **expr {
                return true;
            }
        }
    }
    
    // Check in HAVING clause if it exists
    if let Some(having) = &stmt.having {
        if contains_aggregate_expr(having) {
            return true;
        }
    }
    
    false
}

/// Recursively checks if an expression contains aggregate functions
fn contains_aggregate_expr(expr: &Expression) -> bool {
    match expr {
        Expression::Aggregate { .. } => true,
        Expression::BinaryOp { left, right, .. } => {
            contains_aggregate_expr(left) || contains_aggregate_expr(right)
        }
        Expression::Function { args, .. } => {
            args.iter().any(|arg| contains_aggregate_expr(arg))
        }
        _ => false,
    }
}

/// Build a logical plan from a SELECT statement
pub fn build_logical_plan(stmt: &SelectStatement, catalog: Arc<RwLock<Catalog>>) -> LogicalPlan {
    // Start with the FROM clause - this forms the base of our plan
    if stmt.from.is_empty() {
        // Handle case with no FROM clause (e.g., SELECT 1+1)
        // Here, extract_column_names won't have a source_table_name for wildcard, so "*" would remain "*"
        let projection_cols = extract_column_names(&stmt.columns, None, Some(&catalog));
        return LogicalPlan::Projection {
            columns: projection_cols,
            input: Box::new(LogicalPlan::Scan { // Or a dummy / constant value plan
                table_name: "dual".to_string(), // Use a dummy table
                alias: None,
            }),
        };
    }
    
    // Start with the first (or only) table in the FROM clause
    let base_table_ref = &stmt.from[0];
    let mut plan = LogicalPlan::Scan {
        table_name: base_table_ref.name.clone(),
        alias: base_table_ref.alias.clone(),
    };
    let current_source_table_name_for_wildcard = base_table_ref.name.as_str();

    // Process JOIN clauses if any
    for join in &stmt.joins {
        let right_plan = LogicalPlan::Scan {
            table_name: join.table.name.clone(),
            alias: join.table.alias.clone(),
        };
        
        plan = LogicalPlan::Join {
            left: Box::new(plan),
            right: Box::new(right_plan),
            condition: *join.condition.clone(),
            join_type: join.join_type.clone(),
        };
        // After a join, wildcard expansion becomes more complex (needs to consider columns from all joined tables).
        // For this initial fix, we'll simplify and say wildcard after join is not yet fully schema-aware.
        // Or, if we only support `SELECT T1.*, T2.*`, that's different.
        // For a simple `SELECT *` after join, it would list all columns from all tables.
        // This part is NOT being fully addressed in this immediate edit for simplicity.
        // current_source_table_name_for_wildcard = ???; // This becomes tricky
    }
    
    // If there's a WHERE clause, add a Filter node
    if let Some(where_expr) = &stmt.where_clause {
        plan = LogicalPlan::Filter {
            predicate: (**where_expr).clone(),
            input: Box::new(plan),
        };
    }
    
    // Handle GROUP BY and Aggregation if present
    if has_aggregates(stmt) || stmt.group_by.as_ref().map_or(false, |gb_vec| !gb_vec.is_empty()) {
        let aggregate_expressions = extract_aggregate_expressions(&stmt.columns);
        let projection_names = extract_column_names(&stmt.columns, Some(current_source_table_name_for_wildcard), Some(&catalog));

        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group_by: stmt.group_by.clone().unwrap_or_default(),
            aggregate_expressions,
            having: stmt.having.clone().map(|expr| *expr),
        };
        
        return LogicalPlan::Projection { 
            columns: projection_names, 
            input: Box::new(plan),
        };
    }
    
    // Add a Projection node for the SELECT columns
    // This is the main place where SELECT * expansion for non-aggregate queries will take effect.
    let final_projection_columns = extract_column_names(
        &stmt.columns, 
        Some(current_source_table_name_for_wildcard), // Pass table name for wildcard
        Some(&catalog) // Pass catalog for schema lookup
    );

    LogicalPlan::Projection {
        columns: final_projection_columns,
        input: Box::new(plan),
    }
}

/// Check if a column reference belongs to a specific table
pub fn column_belongs_to_table(col: &ColumnReference, table_name: &str, alias: &Option<String>) -> bool {
    if let Some(table) = &col.table {
        // If column has explicit table name, check if it matches
        if let Some(a) = alias {
            // Check against alias if present
            table == a
        } else {
            // Check against actual table name
            table == table_name
        }
    } else {
        // If no table qualification, assume it belongs to this table
        // This is simplified - in a real system with joins, we'd check the schema
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::ast::{TableReference, Value, Operator};
    
    #[test]
    fn test_simple_logical_plan() {
        // Create a simple SELECT statement
        let stmt = SelectStatement {
            columns: vec![
                SelectColumn::Column(ColumnReference {
                    table: None,
                    name: "id".to_string(),
                }),
                SelectColumn::Column(ColumnReference {
                    table: None,
                    name: "name".to_string(),
                }),
            ],
            from: vec![TableReference {
                name: "users".to_string(),
                alias: None,
            }],
            where_clause: None,
            joins: vec![],
            group_by: None,
            having: None,
        };
        
        // Build logical plan
        let plan = build_logical_plan(&stmt, Arc::new(RwLock::new(Catalog::new())));
        
        // Verify the structure
        if let LogicalPlan::Projection { columns, input } = plan {
            assert_eq!(columns, vec!["id".to_string(), "name".to_string()]);
            
            if let LogicalPlan::Scan { table_name, alias } = *input {
                assert_eq!(table_name, "users");
                assert!(alias.is_none());
            } else {
                panic!("Expected Scan operation under Projection");
            }
        } else {
            panic!("Expected Projection as root operation");
        }
    }
    
    #[test]
    fn test_logical_plan_with_filter() {
        // Create a SELECT statement with a WHERE clause
        let stmt = SelectStatement {
            columns: vec![SelectColumn::Wildcard],
            from: vec![TableReference {
                name: "products".to_string(),
                alias: None,
            }],
            where_clause: Some(Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column(ColumnReference {
                    table: None,
                    name: "price".to_string(),
                })),
                op: Operator::GreaterThan,
                right: Box::new(Expression::Literal(Value::Integer(100))),
            })),
            joins: vec![],
            group_by: None,
            having: None,
        };
        
        // Build logical plan
        let plan = build_logical_plan(&stmt, Arc::new(RwLock::new(Catalog::new())));
        
        // Verify the structure
        if let LogicalPlan::Projection { columns, input } = plan {
            assert_eq!(columns, vec!["*".to_string()]);
            
            if let LogicalPlan::Filter { input, .. } = *input {
                if let LogicalPlan::Scan { table_name, alias } = *input {
                    assert_eq!(table_name, "products");
                    assert!(alias.is_none());
                } else {
                    panic!("Expected Scan operation under Filter");
                }
            } else {
                panic!("Expected Filter operation under Projection");
            }
        } else {
            panic!("Expected Projection as root operation");
        }
    }
    
    #[test]
    fn test_where_clause() {
        // Create a SELECT statement with WHERE clause
        let stmt = SelectStatement {
            columns: vec![
                SelectColumn::Column(ColumnReference {
                    table: None,
                    name: "id".to_string(),
                }),
                SelectColumn::Column(ColumnReference {
                    table: None,
                    name: "name".to_string(),
                }),
            ],
            from: vec![TableReference {
                name: "users".to_string(),
                alias: None,
            }],
            where_clause: Some(Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column(ColumnReference {
                    table: None,
                    name: "id".to_string(),
                })),
                op: Operator::Equals,
                right: Box::new(Expression::Literal(Value::Integer(1))),
            })),
            joins: vec![],
            group_by: None,
            having: None,
        };
    }

    #[test]
    fn test_filter_plan() {
        // Create a SELECT statement with WHERE clause
        let stmt = SelectStatement {
            columns: vec![
                SelectColumn::Column(ColumnReference {
                    table: None,
                    name: "id".to_string(),
                }),
                SelectColumn::Column(ColumnReference {
                    table: None,
                    name: "name".to_string(),
                }),
            ],
            from: vec![TableReference {
                name: "users".to_string(),
                alias: None,
            }],
            where_clause: Some(Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Column(ColumnReference {
                    table: None,
                    name: "id".to_string(),
                })),
                op: Operator::GreaterThan,
                right: Box::new(Expression::Literal(Value::Integer(100))),
            })),
            joins: vec![],
            group_by: None,
            having: None,
        };
        
        // Build logical plan
        let plan = build_logical_plan(&stmt, Arc::new(RwLock::new(Catalog::new())));
        
        // Verify the structure - should have Filter between Projection and Scan
        if let LogicalPlan::Projection { columns: _, input } = plan {
            if let LogicalPlan::Filter { predicate: _, input } = *input {
                if let LogicalPlan::Scan { table_name, alias } = *input {
                    assert_eq!(table_name, "users");
                    assert!(alias.is_none());
                } else {
                    panic!("Expected Scan operation under Filter");
                }
            } else {
                panic!("Expected Filter operation under Projection");
            }
        } else {
            panic!("Expected Projection as root operation");
        }
    }

    #[test]
    fn test_simple_plan() {
        // Create a simple SELECT statement
        let stmt = SelectStatement {
            columns: vec![
                SelectColumn::Column(ColumnReference {
                    table: None,
                    name: "id".to_string(),
                }),
            ],
            from: vec![TableReference {
                name: "test".to_string(),
                alias: None,
            }],
            where_clause: None,
            joins: vec![],
            group_by: None,
            having: None,
        };
        
        // Build logical plan
        let plan = build_logical_plan(&stmt, Arc::new(RwLock::new(Catalog::new())));
        
        // Should be a simple Projection -> Scan
        match plan {
            LogicalPlan::Projection { columns, input } => {
                assert_eq!(columns, vec!["id".to_string()]);
                
                match *input {
                    LogicalPlan::Scan { table_name, .. } => {
                        assert_eq!(table_name, "test");
                    },
                    _ => panic!("Expected Scan under Projection"),
                }
            },
            _ => panic!("Expected Projection"),
        }
    }

    #[test]
    fn test_logical_plan_with_group_by() {
        // Create a SELECT statement with a GROUP BY clause
        let stmt = SelectStatement {
            columns: vec![
                SelectColumn::Column(ColumnReference {
                    table: None,
                    name: "department_id".to_string(),
                }),
                SelectColumn::Expression { 
                    expr: Box::new(Expression::Aggregate { 
                        function: AggregateFunction::Count, 
                        arg: None,
                    }),
                    alias: Some("count".to_string()),
                },
            ],
            from: vec![TableReference {
                name: "employees".to_string(),
                alias: None,
            }],
            where_clause: None,
            joins: vec![],
            group_by: Some(vec![
                Expression::Column(ColumnReference {
                    table: None,
                    name: "department_id".to_string(),
                }),
            ]),
            having: None,
        };
        
        // Build logical plan
        let plan = build_logical_plan(&stmt, Arc::new(RwLock::new(Catalog::new())));
        
        // Verify the structure
        if let LogicalPlan::Projection { columns, input } = plan {
            assert_eq!(columns, vec!["department_id".to_string(), "count".to_string()]);
            
            if let LogicalPlan::Aggregate { input: agg_input, group_by, aggregate_expressions, having } = *input {
                assert_eq!(group_by.len(), 1);
                assert_eq!(aggregate_expressions.len(), 1);
                assert!(having.is_none());
                
                if let LogicalPlan::Scan { table_name, alias } = *agg_input {
                    assert_eq!(table_name, "employees");
                    assert!(alias.is_none());
                } else {
                    panic!("Expected Scan operation under Aggregate");
                }
            } else {
                panic!("Expected Aggregate operation under Projection");
            }
        } else {
            panic!("Expected Projection as root operation");
        }
    }
    
    #[test]
    fn test_logical_plan_with_having() {
        // Create a SELECT statement with GROUP BY and HAVING clauses
        let stmt = SelectStatement {
            columns: vec![
                SelectColumn::Column(ColumnReference {
                    table: None,
                    name: "department_id".to_string(),
                }),
                SelectColumn::Expression { 
                    expr: Box::new(Expression::Aggregate { 
                        function: AggregateFunction::Count, 
                        arg: None,
                    }),
                    alias: Some("count".to_string()),
                },
            ],
            from: vec![TableReference {
                name: "employees".to_string(),
                alias: None,
            }],
            where_clause: None,
            joins: vec![],
            group_by: Some(vec![
                Expression::Column(ColumnReference {
                    table: None,
                    name: "department_id".to_string(),
                }),
            ]),
            having: Some(Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Aggregate { 
                    function: AggregateFunction::Count, 
                    arg: None,
                }),
                op: Operator::GreaterThan,
                right: Box::new(Expression::Literal(Value::Integer(5))),
            })),
        };
        
        // Build logical plan
        let plan = build_logical_plan(&stmt, Arc::new(RwLock::new(Catalog::new())));
        
        // Verify the structure
        if let LogicalPlan::Projection { columns, input } = plan {
            assert_eq!(columns, vec!["department_id".to_string(), "count".to_string()]);
            
            if let LogicalPlan::Aggregate { input: agg_input, group_by, aggregate_expressions, having } = *input {
                assert_eq!(group_by.len(), 1);
                assert_eq!(aggregate_expressions.len(), 1);
                assert!(having.is_some());
                
                if let LogicalPlan::Scan { table_name, alias } = *agg_input {
                    assert_eq!(table_name, "employees");
                    assert!(alias.is_none());
                } else {
                    panic!("Expected Scan operation under Aggregate");
                }
            } else {
                panic!("Expected Aggregate operation under Projection");
            }
        } else {
            panic!("Expected Projection as root operation");
        }
    }
    
    #[test]
    fn test_logical_plan_with_multiple_aggregates() {
        // Create a SELECT statement with multiple aggregate functions
        let stmt = SelectStatement {
            columns: vec![
                SelectColumn::Column(ColumnReference {
                    table: None,
                    name: "department_id".to_string(),
                }),
                SelectColumn::Expression { 
                    expr: Box::new(Expression::Aggregate { 
                        function: AggregateFunction::Count, 
                        arg: None,
                    }),
                    alias: Some("count".to_string()),
                },
                SelectColumn::Expression { 
                    expr: Box::new(Expression::Aggregate { 
                        function: AggregateFunction::Sum, 
                        arg: Some(Box::new(Expression::Column(ColumnReference {
                            table: None,
                            name: "salary".to_string(),
                        }))),
                    }),
                    alias: Some("total_salary".to_string()),
                },
                SelectColumn::Expression { 
                    expr: Box::new(Expression::Aggregate { 
                        function: AggregateFunction::Avg, 
                        arg: Some(Box::new(Expression::Column(ColumnReference {
                            table: None,
                            name: "salary".to_string(),
                        }))),
                    }),
                    alias: Some("avg_salary".to_string()),
                },
            ],
            from: vec![TableReference {
                name: "employees".to_string(),
                alias: None,
            }],
            where_clause: None,
            joins: vec![],
            group_by: Some(vec![
                Expression::Column(ColumnReference {
                    table: None,
                    name: "department_id".to_string(),
                }),
            ]),
            having: None,
        };
        
        // Build logical plan
        let plan = build_logical_plan(&stmt, Arc::new(RwLock::new(Catalog::new())));
        
        // Verify the structure
        if let LogicalPlan::Projection { columns, input } = plan {
            assert_eq!(columns, vec![
                "department_id".to_string(), 
                "count".to_string(), 
                "total_salary".to_string(), 
                "avg_salary".to_string()
            ]);
            
            if let LogicalPlan::Aggregate { input: agg_input, group_by, aggregate_expressions, .. } = *input {
                assert_eq!(group_by.len(), 1);
                assert_eq!(aggregate_expressions.len(), 3); // COUNT, SUM, AVG
                
                // Verify each aggregate function
                let mut has_count = false;
                let mut has_sum = false;
                let mut has_avg = false;
                
                for expr in &aggregate_expressions {
                    if let Expression::Aggregate { function, .. } = expr {
                        match function {
                            AggregateFunction::Count => has_count = true,
                            AggregateFunction::Sum => has_sum = true,
                            AggregateFunction::Avg => has_avg = true,
                            _ => {}
                        }
                    }
                }
                
                assert!(has_count, "COUNT not found");
                assert!(has_sum, "SUM not found");
                assert!(has_avg, "AVG not found");
                
                if let LogicalPlan::Scan { table_name, .. } = *agg_input {
                    assert_eq!(table_name, "employees");
                } else {
                    panic!("Expected Scan operation under Aggregate");
                }
            } else {
                panic!("Expected Aggregate operation under Projection");
            }
        } else {
            panic!("Expected Projection as root operation");
        }
    }
} 