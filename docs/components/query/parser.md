# SQL Parser Component

The SQL parser is responsible for parsing SQL queries into an abstract syntax tree (AST) that can be processed by the query planner. This document describes the implementation and capabilities of the BayunDB SQL parser.

## Architecture

The parser is implemented as a recursive descent parser with the following components:

- **Lexer** (`lexer.rs`): Tokenizes the input SQL string into a stream of tokens.
- **Parser** (`parser.rs`): Constructs an AST from the token stream.
- **AST** (`ast.rs`): Defines the abstract syntax tree structures that represent SQL queries.

## Supported SQL Syntax

The parser currently supports the following SQL syntax:

### SELECT Statements
```sql
SELECT <column-list> FROM <table-list>
  [WHERE <condition>]
  [JOIN <table> ON <join-condition>]
  [LEFT JOIN <table> ON <join-condition>]
  [GROUP BY <column-list>]
  [HAVING <condition>]
```

#### Column List
The column list can include:
- Individual columns: `column_name`
- Qualified columns: `table_name.column_name`
- Wildcard: `*`
- Expressions: `column_name + 10`
- Aggregate functions: `COUNT(*)`, `SUM(column_name)`, `AVG(column_name)`, etc.
- Aliased columns: `column_name AS alias_name`

#### Aggregate Functions
The parser supports the following aggregate functions:
- `COUNT(*)`: Count all rows
- `COUNT(column)`: Count non-null values in the column
- `SUM(column)`: Calculate sum of values
- `AVG(column)`: Calculate average of values
- `MIN(column)`: Find minimum value
- `MAX(column)`: Find maximum value

#### GROUP BY Clause
The GROUP BY clause allows grouping rows based on one or more columns:
```sql
SELECT department_id, COUNT(*) FROM employees GROUP BY department_id
```

Multiple grouping columns are supported:
```sql
SELECT department_id, job_title, COUNT(*) 
FROM employees 
GROUP BY department_id, job_title
```

#### HAVING Clause
The HAVING clause filters groups after aggregation:
```sql
SELECT department_id, COUNT(*) 
FROM employees 
GROUP BY department_id 
HAVING COUNT(*) > 5
```

### WHERE Conditions
- Comparison operators: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`
- Logical operators: `AND`, `OR`
- Parenthesized conditions

### JOIN Types
- INNER JOIN (default)
- LEFT OUTER JOIN
- RIGHT OUTER JOIN (syntax supported, but implementation pending)
- FULL OUTER JOIN (syntax supported, but implementation pending)

### Data Manipulation Language (DML)
- INSERT (syntax defined in AST, parser implementation pending)
- UPDATE (syntax defined in AST, parser implementation pending)
- DELETE (syntax defined in AST, parser implementation pending)

### Data Definition Language (DDL)
- CREATE TABLE (syntax defined in AST, parser implementation pending)

## Implementation Status

| Feature | Status | Notes |
|---------|--------|-------|
| Basic SELECT | ✅ Complete | |
| Column references | ✅ Complete | |
| WHERE predicates | ✅ Complete | |
| JOIN operations | ✅ Complete | Both INNER and LEFT JOIN supported |
| GROUP BY | ✅ Complete | Parser support implemented |
| HAVING | ✅ Complete | Parser support implemented |
| Aggregate functions | ✅ Complete | Parser support for COUNT, SUM, AVG, MIN, MAX |
| INSERT/UPDATE/DELETE | ⏳ Pending | AST structures defined, parser not implemented |
| CREATE TABLE | ⏳ Pending | AST structures defined, parser not implemented |

## Usage

The parser can be used as follows:

```rust
use bayundb::query::parser::Parser;
use bayundb::query::parser::ast::Statement;

let sql = "SELECT id, name FROM users WHERE age > 18";
let mut parser = Parser::new(sql);
match parser.parse_statement() {
    Ok(stmt) => {
        // Process the AST statement
        if let Statement::Select(select) = stmt {
            // Process SELECT statement
        }
    },
    Err(err) => {
        // Handle parsing error
    }
}
```

## Error Handling

The parser provides detailed error messages for syntax errors, including:
- Unexpected tokens
- Missing expected tokens
- Invalid syntax structures
- End of input errors

## Future Enhancements

Planned enhancements to the parser include:
- Implementation of DDL operations (CREATE, ALTER, DROP)
- Support for subqueries
- Support for UNION, INTERSECT, EXCEPT
- Support for window functions
- Improved error recovery
- More comprehensive syntax validation 