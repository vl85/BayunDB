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