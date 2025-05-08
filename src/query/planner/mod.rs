// Query Planner Module
//
// This module is responsible for translating parsed SQL statements into
// executable query plans. It includes both logical and physical planning.

// Re-export public components
pub mod logical;
pub mod physical;
pub mod optimizer;

// Export key types
pub use self::logical::LogicalPlan;
pub use self::physical::PhysicalPlan;
pub use self::optimizer::Optimizer;

use crate::query::parser::ast::Statement;
use crate::query::parser::Parser;
use crate::query::executor::result::QueryResult;

/// The Planner is responsible for translating SQL statements into executable query plans
pub struct Planner {
    optimizer: Optimizer,
}

impl Planner {
    /// Create a new query planner
    pub fn new() -> Self {
        Planner {
            optimizer: Optimizer::new(),
        }
    }
    
    /// Create a logical plan from a SQL query string
    pub fn create_logical_plan_from_sql(&self, sql: &str) -> QueryResult<LogicalPlan> {
        // Parse the SQL into an AST
        let mut parser = Parser::new(sql);
        let stmt = parser.parse_statement()
            .map_err(|e| crate::query::executor::result::QueryError::PlanningError(
                format!("Parse error: {:?}", e)
            ))?;
        
        // Convert the AST to a logical plan
        let plan = self.create_logical_plan(&stmt)?;
        
        // Apply optimization rules
        let optimized_plan = self.optimizer.optimize(plan);
        
        Ok(optimized_plan)
    }
    
    /// Create a logical plan from a statement
    pub fn create_logical_plan(&self, stmt: &Statement) -> QueryResult<LogicalPlan> {
        match stmt {
            Statement::Select(select_stmt) => Ok(logical::build_logical_plan(select_stmt)),
            _ => Err(crate::query::executor::result::QueryError::PlanningError(
                format!("Unsupported statement type: {}", stmt)
            )),
        }
    }
    
    /// Create a physical plan from a logical plan
    pub fn create_physical_plan(&self, logical_plan: &LogicalPlan) -> QueryResult<PhysicalPlan> {
        Ok(physical::create_physical_plan(logical_plan))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_end_to_end_planning() {
        let planner = Planner::new();
        
        // Simple SQL query
        let sql = "SELECT id, name FROM users WHERE id > 100";
        let plan = planner.create_logical_plan_from_sql(sql).unwrap();
        
        // Check the structure of the plan
        if let LogicalPlan::Projection { columns, input } = plan {
            assert_eq!(columns, vec!["id", "name"]);
            
            if let LogicalPlan::Filter { input, .. } = *input {
                if let LogicalPlan::Scan { table_name, .. } = *input {
                    assert_eq!(table_name, "users");
                } else {
                    panic!("Expected Scan under Filter");
                }
            } else {
                panic!("Expected Filter under Projection");
            }
        } else {
            panic!("Expected Projection as root operation");
        }
    }
} 