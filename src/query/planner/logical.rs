// Logical Query Plan Implementation
//
// This module defines the logical plan representation for query processing.

use std::fmt;

use crate::query::parser::ast::{Expression, SelectStatement, SelectColumn, ColumnReference, JoinType};

/// Represents a node in the logical query plan
#[derive(Debug, Clone)]
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
        }
    }
}

/// Extract column names from SelectColumn variants
fn extract_column_names(columns: &[SelectColumn]) -> Vec<String> {
    let mut result = Vec::new();
    
    for col in columns {
        match col {
            SelectColumn::Wildcard => {
                // Wildcard is handled differently - we'll need schema information
                // from the catalog later. For now, use a placeholder.
                result.push("*".to_string());
            }
            SelectColumn::Column(col_ref) => {
                result.push(col_ref.name.clone());
            }
            SelectColumn::Expression { expr: _, alias } => {
                if let Some(alias_name) = alias {
                    result.push(alias_name.clone());
                } else {
                    // For an expression without an alias, we'd ideally generate a name
                    // For now, use a placeholder
                    result.push("expr".to_string());
                }
            }
        }
    }
    
    result
}

/// Build a logical plan from a SELECT statement
pub fn build_logical_plan(stmt: &SelectStatement) -> LogicalPlan {
    // Start with the FROM clause - this forms the base of our plan
    if stmt.from.is_empty() {
        // Handle case with no FROM clause (rare in SQL)
        return LogicalPlan::Projection {
            columns: extract_column_names(&stmt.columns),
            input: Box::new(LogicalPlan::Scan {
                table_name: "dual".to_string(), // Use a dummy table
                alias: None,
            }),
        };
    }
    
    // Start with the first (or only) table in the FROM clause
    let base_table = &stmt.from[0];
    let mut plan = LogicalPlan::Scan {
        table_name: base_table.name.clone(),
        alias: base_table.alias.clone(),
    };
    
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
    }
    
    // If there's a WHERE clause, add a Filter node
    if let Some(where_expr) = &stmt.where_clause {
        plan = LogicalPlan::Filter {
            predicate: (**where_expr).clone(),
            input: Box::new(plan),
        };
    }
    
    // Finally, add a Projection for the SELECT clause
    LogicalPlan::Projection {
        columns: extract_column_names(&stmt.columns),
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
        let plan = build_logical_plan(&stmt);
        
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
        let plan = build_logical_plan(&stmt);
        
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
        let plan = build_logical_plan(&stmt);
        
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
        let plan = build_logical_plan(&stmt);
        
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
} 