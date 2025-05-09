# Schema Management in BayunDB

This document outlines the schema management capabilities in BayunDB, which enable the definition, modification, and deletion of database objects such as schemas, tables, and columns.

## Overview

Schema management is a foundational feature of any database system, providing the structure that defines how data is organized and stored. In BayunDB, the schema management system encompasses:

1. **Catalogs** - The top-level container for database metadata
2. **Schemas** - Collections of database objects (primarily tables)
3. **Tables** - Defined structures for storing data
4. **Columns** - Individual data elements within tables

## Components

### Catalog

The Catalog is the central repository for all database schema information. It manages:

- Multiple schema objects
- The current active schema
- Table creation and lookup

Key operations:
- Creating a new schema
- Setting the current schema
- Creating and querying tables

### Schema

A Schema is a collection of related tables. It provides:

- Namespace isolation for table names
- Grouping of related database objects

Key operations:
- Adding tables to the schema
- Retrieving tables by name
- Listing all tables in the schema

### Table

A Table represents the structure for storing data. It manages:

- Column definitions
- Primary key information
- Page references for stored data

Key operations:
- Adding columns to the table
- Retrieving column information
- Calculating row size 
- Getting storage information

### Column

A Column defines a single data element within a table. It includes:

- Data type information
- Constraints (primary key, nullable)
- Default values

## Supported SQL Operations

BayunDB currently supports the following schema management operations:

### CREATE TABLE

```sql
CREATE TABLE table_name (
  column_name data_type [constraints],
  ...
);
```

Supported data types:
- INTEGER
- FLOAT
- TEXT
- BOOLEAN
- DATE
- TIMESTAMP
- BLOB

Supported constraints:
- PRIMARY KEY
- NOT NULL

Example:
```sql
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT,
  created_at TIMESTAMP
);
```

## Implementation Details

### Storage

Schema information is stored in memory in the current implementation. Future versions will persist this information to disk in system tables.

### Metadata Management

The catalog uses a hierarchical structure:
- Catalog contains multiple schemas
- Each schema contains multiple tables
- Each table contains multiple columns

### Integration with Query Processing

Schema information is used during:
1. Query parsing - to validate table and column references
2. Query planning - to determine data types and access methods
3. Query execution - to locate data and interpret stored values

## Future Enhancements

1. **Schema Persistence** - Storing metadata in system tables for durability
2. **ALTER TABLE** - Support for modifying existing table definitions
3. **DROP TABLE** - Support for removing tables
4. **Advanced Constraints** - Support for UNIQUE, FOREIGN KEY, CHECK constraints
5. **Indexes** - Integrated index management
6. **Views** - Support for stored queries as virtual tables

## API Reference

### Catalog API

```rust
// Create a new catalog
let catalog = Catalog::new();

// Create a new schema
catalog.create_schema("my_schema")?;

// Set the current schema
catalog.set_current_schema("my_schema")?;

// Create a table
let columns = vec![
    Column::new("id".to_string(), DataType::Integer, false, true, None),
    Column::new("name".to_string(), DataType::Text, false, false, None),
];
let table = Table::new("users".to_string(), columns);
catalog.create_table(table)?;

// Get a table
let users_table = catalog.get_table("users")?;
``` 