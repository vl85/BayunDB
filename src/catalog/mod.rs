//! Catalog Management Module
//!
//! This module manages the database schema metadata, including tables, columns, indexes, and other database objects.

pub mod schema;
pub mod table;
pub mod column;
pub mod validation;
pub mod validation_error;
pub mod value;
pub mod compatibility;
pub mod expression;
pub mod convert;
pub mod catalog;
pub mod alter_table;
pub mod schema_management;
pub mod table_column_ops;

// Re-export key types
pub use self::schema::Schema;
pub use self::table::Table;
pub use self::column::Column;
pub use self::schema::DataType;
pub use self::validation::{TypeValidator};
pub use self::validation_error::{ValidationError, ValidationResult};
pub use self::catalog::Catalog;

use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU32, Ordering};
use crate::query::executor::result::QueryError;

// Global catalog instance using a thread-safe lazy initialization
static CATALOG_INSTANCE: Lazy<Arc<RwLock<Catalog>>> = Lazy::new(|| {
    let mut schemas = HashMap::new();
    
    // Create default schema
    let default_schema = Schema::new("public".to_string());
    schemas.insert("public".to_string(), default_schema);
    
    let catalog = Catalog {
        schemas: RwLock::new(schemas),
        current_schema: RwLock::new("public".to_string()),
        table_id_counter: AtomicU32::new(1),
    };
    
    Arc::new(RwLock::new(catalog))
});

// Tests module
#[cfg(test)]
mod tests {
// // ... existing code ...
}

#[cfg(test)]
pub mod validation_tests; 