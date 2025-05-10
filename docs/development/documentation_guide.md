# BayunDB Documentation Guide

This guide provides conventions and best practices for writing and maintaining documentation for the BayunDB project.

## 1. Philosophy

*   **Clarity and Conciseness:** Documentation should be easy to understand and to the point.
*   **Target Audience:** Consider the audience (end-users, new contributors, experienced developers) when deciding on the level of detail.
*   **Public API First:** Prioritize thorough documentation for all public APIs.
*   **Explain the "Why":** For complex logic or design choices, explain the rationale behind them.
*   **Avoid Redundancy:** Don't comment on obvious code. The code itself should be as clear as possible.
*   **Keep it Current:** Documentation should evolve with the codebase.

## 2. What to Document

### 2.1. Module-Level Documentation (`//!`)

Each significant module (typically `mod.rs` or the main `.rs` file for a component) should have a module-level doc comment at the top:

*   Start with `//!`.
*   Briefly describe the module's overall purpose and responsibility.
*   Mention key structs, enums, traits, or functions it exports.
*   Briefly describe how it interacts with other major components if applicable.

Example:

```rust
//! src/query/planner/logical.rs
//! 
//! Defines the logical query plan representation for query processing.
//! This module includes the `LogicalPlan` enum and functions to build
//! this plan from an Abstract Syntax Tree (AST).
```

### 2.2. Public Item Documentation (`///`)

All public items (structs, enums, functions, methods, traits, public fields within structs) require documentation:

*   Use `///` for item-level doc comments.
*   **Summary Line:** Start with a concise one-sentence summary of the item's purpose.
*   **Detailed Explanation (if needed):** For complex items, follow the summary with more paragraphs.
    *   **Functions/Methods:**
        *   Explain what the function does.
        *   Describe each parameter, its type, and its purpose.
        *   Describe the return value, including success scenarios (`Ok(T)`) and potential error conditions (`Err(E)` for `QueryResult<T>`).
        *   Document any potential panics (and if they are considered part of the API contract or a bug).
        *   Mention any important pre-conditions or post-conditions.
        *   State if the function has side effects.
    *   **Structs/Enums:**
        *   Explain the purpose of the struct/enum.
        *   Document each public field or variant, explaining its meaning and use.
*   **Examples (````rust ... ````):** Include short, runnable examples (doc tests) where they significantly aid understanding, especially for library APIs.
*   **Safety:** For `unsafe` code, clearly document the safety invariants that callers must uphold.
*   **Markdown:** Use Markdown for formatting (e.g., backticks for code `identifier`, bold/italics, lists).

Example:

```rust
/// Represents a node in the logical query plan.
///
/// Each variant corresponds to a specific logical operation in a query,
/// such as scanning a table, filtering rows, or joining relations.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Scans a table with an optional alias.
    Scan {
        /// The name of the table to be scanned.
        table_name: String,
        /// An optional alias for the table.
        alias: Option<String>,
    },
    // ... other variants ...
}

/// Creates a new table scan operator.
///
/// # Arguments
///
/// * `catalog`: A shared reference to the database catalog for schema lookups.
/// * `table_name`: The name of the table to scan.
/// * `table_alias`: An optional alias for the table, used for qualifying column names.
///
/// # Returns
///
/// A `QueryResult` containing the `TableScanOperator` or a `QueryError` if
/// the table does not exist or schema lookup fails.
///
/// # Examples
///
/// ```
/// // This is a conceptual example; actual setup would be more involved.
/// // let catalog = ...;
/// // let scan_op = create_table_scan(catalog, "users".to_string(), Some("u".to_string()));
/// // assert!(scan_op.is_ok());
/// ```
pub fn create_table_scan(
    catalog: Arc<RwLock<Catalog>>,
    table_name: String,
    table_alias: Option<String>,
) -> QueryResult<Arc<Mutex<dyn Operator + Send>>> {
    // ... implementation ...
}
```

### 2.3. Internal Logic & Design Decisions

*   For complex algorithms or non-obvious design choices within private functions or modules, use standard `//` comments to explain the "why" or the high-level steps. This is for maintainers.

## 3. Style and Formatting

*   Write in clear, grammatically correct English.
*   Use complete sentences.
*   Format doc comments to be readable both in source code and as rendered HTML.
*   Follow Rust naming conventions.

## 4. `README.md`

The main `README.md` at the project root should:
*   Provide a project overview.
*   Include build and run instructions.
*   Briefly explain the project structure.
*   Link to more detailed documentation in the `docs/` directory.
*   Cover CLI usage.
*   Summarize testing and benchmarking strategies.

## 5. Keeping Documentation Up-to-Date

*   When code is changed (especially public APIs), the corresponding documentation **must** be updated in the same commit/PR.
*   Review documentation प्यारt of the code review process.

## 6. Tools

*   **`cargo doc --open`**: Build and view the HTML documentation locally.
*   **`cargo test`**: Runs doc tests embedded in the documentation.
*   **Linter (Clippy):** Can be configured to warn about missing documentation for public items.

By following these guidelines, we can maintain high-quality, useful documentation for BayunDB. 