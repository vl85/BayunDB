// Query Planner Module
//
// This module is responsible for translating parsed SQL statements into
// executable query plans. It includes both logical and physical planning.

// Re-export public components
pub mod logical;
pub mod physical;
pub mod physical_plan;
pub mod physical_optimizer;
pub mod operator_builder;
pub mod cost_model;
pub mod optimizer;

// Export key types
pub use self::logical::LogicalPlan;
pub use self::physical_plan::PhysicalPlan;
pub use self::optimizer::Optimizer;
pub use self::physical_optimizer::PhysicalOptimizer;
pub use self::operator_builder::OperatorBuilder;
pub use self::cost_model::CostModel;

use crate::query::parser::ast::Statement;
use crate::query::parser::parse;
use crate::query::executor::result::QueryResult;
use std::sync::Arc;
use crate::storage::buffer::BufferPoolManager;
use crate::catalog::Catalog;
use std::sync::RwLock;

/// The Planner is responsible for translating SQL statements into executable query plans
pub struct Planner {
    optimizer: Optimizer,
    physical_optimizer: PhysicalOptimizer,
    operator_builder: OperatorBuilder,
    cost_model: CostModel,
    catalog: Arc<RwLock<Catalog>>,
}

impl Planner {
    /// Create a new query planner
    pub fn new(buffer_pool: Arc<BufferPoolManager>, catalog: Arc<RwLock<Catalog>>) -> Self {
        Planner {
            optimizer: Optimizer::new(),
            physical_optimizer: PhysicalOptimizer::new(),
            operator_builder: OperatorBuilder::new(buffer_pool.clone(), catalog.clone()),
            cost_model: CostModel::new(),
            catalog: catalog,
        }
    }
    
    /// Create a logical plan from a SQL query string
    pub fn create_logical_plan_from_sql(&self, sql: &str) -> QueryResult<LogicalPlan> {
        // Parse the SQL into an AST
        let stmt = parse(sql)
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
            Statement::Select(select_stmt) => Ok(logical::build_logical_plan(select_stmt, self.catalog.clone())),
            Statement::Create(create_stmt) => Ok(LogicalPlan::CreateTable {
                table_name: create_stmt.table_name.clone(),
                columns: create_stmt.columns.clone(),
            }),
            Statement::Alter(alter_stmt) => Ok(LogicalPlan::AlterTable {
                statement: alter_stmt.clone(), // Clone the AlterTableStatement
            }),
            _ => Err(crate::query::executor::result::QueryError::PlanningError(
                format!("Unsupported statement type in logical planner: {}", stmt)
            )),
        }
    }
    
    /// Create a physical plan from a logical plan
    pub fn create_physical_plan(&self, logical_plan: &LogicalPlan) -> QueryResult<PhysicalPlan> {
        // Convert logical plan to a basic physical plan
        let physical_plan = physical::create_physical_plan(logical_plan);
        
        // Apply physical plan optimizations
        let optimized_physical_plan = self.physical_optimizer.optimize(physical_plan);
        
        Ok(optimized_physical_plan)
    }
    
    /// Build an operator tree from a physical plan
    pub fn build_operator_tree(&self, physical_plan: &PhysicalPlan) -> QueryResult<Arc<std::sync::Mutex<dyn crate::query::executor::operators::Operator + Send>>> {
        self.operator_builder.build_operator_tree(physical_plan)
    }
    
    /// Estimate the cost of a physical plan
    pub fn estimate_cost(&self, physical_plan: &PhysicalPlan) -> f64 {
        self.cost_model.estimate_cost(physical_plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::ast::{Expression, ColumnReference, Operator, Value, JoinType};
    
    // Helper function to create a buffer pool for testing
    fn create_test_buffer_pool() -> Arc<BufferPoolManager> {
        // Create a temporary file path
        let temp_path = std::env::temp_dir().join("test_db.db");
        let path_str = temp_path.to_str().unwrap().to_string();
        
        // Create buffer pool
        Arc::new(BufferPoolManager::new(100, path_str).unwrap())
    }
    
    #[test]
    fn test_end_to_end_planning() {
        let buffer_pool = create_test_buffer_pool();
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        let planner = Planner::new(buffer_pool, catalog);
        
        // Simple SQL query
        let sql = "SELECT id, name FROM users WHERE age > 25";
        
        // Parse the SQL
        let stmt = parse(sql).unwrap();
        
        // Create a logical plan
        let logical_plan = planner.create_logical_plan(&stmt).unwrap();
        
        // Verify logical plan structure
        assert!(logical_plan.to_string().contains("Filter"));
        assert!(logical_plan.to_string().contains("Projection"));
        
        // Create a physical plan
        let physical_plan = planner.create_physical_plan(&logical_plan).unwrap();
        
        // Verify physical plan structure
        assert!(physical_plan.to_string().contains("Filter"));
        assert!(physical_plan.to_string().contains("Project"));
    }
    
    #[test]
    fn test_invalid_sql() {
        let buffer_pool = create_test_buffer_pool();
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        let planner = Planner::new(buffer_pool, catalog);
        
        // Invalid SQL (missing FROM clause)
        let sql = "SELECT id, name WHERE age > 25";
        
        // Parse the SQL should fail
        let stmt_result = parse(sql);
        assert!(stmt_result.is_err());
    }
    
    #[test]
    fn test_unsupported_statement() {
        let buffer_pool = create_test_buffer_pool();
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        let planner = Planner::new(buffer_pool, catalog);
        
        // Try to parse an unsupported statement type
        let sql = "DROP TABLE users";
        
        // This should parse but the planner won't support it
        let stmt_result = parse(sql);
        if let Ok(stmt) = stmt_result {
            let plan_result = planner.create_logical_plan(&stmt);
            assert!(plan_result.is_err());
        } else {
            // If the parser doesn't even support DROP TABLE, that's okay too
            assert!(stmt_result.is_err());
        }
    }
    
    #[test]
    fn test_create_table_planning() {
        let buffer_pool = create_test_buffer_pool();
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        let planner = Planner::new(buffer_pool, catalog);
        
        // CREATE TABLE SQL
        let sql = "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)";
        
        // Parse the SQL
        let stmt = parse(sql).unwrap();
        
        // Create a logical plan
        let logical_plan = planner.create_logical_plan(&stmt).unwrap();
        
        // Create a physical plan
        let physical_plan = planner.create_physical_plan(&logical_plan).unwrap();
        
        // Verify it's a CreateTable plan
        assert!(physical_plan.to_string().contains("CreateTable"));
    }
    
    #[test]
    fn test_complex_query_planning() {
        let buffer_pool = create_test_buffer_pool();
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        let planner = Planner::new(buffer_pool, catalog);
        
        // Complex query with joins, grouping, and aggregation
        let sql = "SELECT users.name, COUNT(*) FROM users JOIN orders ON users.id = orders.user_id WHERE users.age > 25 GROUP BY users.name";
        
        // Parse the SQL
        let stmt = parse(sql).unwrap();
        
        // Create a logical plan
        let logical_plan = planner.create_logical_plan(&stmt).unwrap();
        
        // Create a physical plan
        let physical_plan = planner.create_physical_plan(&logical_plan).unwrap();
        
        // Verify the physical plan structure
        let plan_str = physical_plan.to_string();
        assert!(plan_str.contains("Aggregate") || plan_str.contains("Group"));
        assert!(plan_str.contains("Join"));
        assert!(plan_str.contains("Filter") || plan_str.contains("Selection"));
    }
    
    #[test]
    fn test_optimization_applied() {
        let buffer_pool = create_test_buffer_pool();
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        let planner = Planner::new(buffer_pool, catalog);
        
        // Create a logical plan that should trigger certain optimizations
        let sql = "SELECT * FROM users WHERE id = 1 AND name = 'test' AND id > 0";
        
        // Parse the SQL
        let stmt = parse(sql).unwrap();
        
        // Create a logical plan
        let logical_plan = planner.create_logical_plan(&stmt).unwrap();
        
        // Create a physical plan with optimizations
        let physical_plan = planner.create_physical_plan(&logical_plan).unwrap();
        
        // Optimizations like predicate pushdown and constant folding should be applied
        // For now, just check that the plan was created successfully
        assert!(!physical_plan.to_string().is_empty());
    }
} 