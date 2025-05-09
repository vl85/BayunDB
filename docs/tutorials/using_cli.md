# Using the BayunDB CLI

This tutorial provides a step-by-step guide to using the BayunDB Command-Line Interface (CLI) for common database operations.

## Prerequisites

- BayunDB installed from source
- Basic understanding of SQL

## Installation

The CLI is built as part of the main BayunDB project:

```bash
# Build the CLI
cargo build --bin bnql
```

This creates the `bnql` executable in the `target/debug` directory.

## Starting the CLI

### Interactive Shell

To start an interactive shell session:

```bash
./target/debug/bnql
```

You should see a welcome message and a prompt:

```
Welcome to BayunDB CLI. Type 'help' for assistance or 'exit' to quit.
bayundb> 
```

### Using a Custom Database File

By default, BayunDB uses `database.db` in the current directory. To specify a different database file:

```bash
./target/debug/bnql --db-path my_custom_database.db
```

## Basic Database Operations

### Creating a New Database

You can create a new database using the `create` command:

```bash
./target/debug/bnql create
```

This initializes a new database file at the specified location (or the default `database.db`).

### Viewing Database Information

To display information about the current database:

```bash
./target/debug/bnql info
```

This will show details such as the database file path, log directory, and buffer pool size.

## Working with SQL

### Executing a Single Query

To execute a single SQL query without entering the interactive shell:

```bash
./target/debug/bnql query "SELECT * FROM users"
```

The results will be displayed in a formatted table:

```
| id | name    | email           |
+----+---------+-----------------+
| 1  | "Alice" | "alice@test.com" |
| 2  | "Bob"   | "bob@test.com"   |
(2 rows)
```

### Creating Tables

In the interactive shell:

```
bayundb> CREATE TABLE users (id INTEGER, name TEXT, email TEXT);
Success: Table created
```

### Inserting Data

```
bayundb> INSERT INTO users VALUES (1, 'Alice', 'alice@test.com');
Success: 1 row inserted

bayundb> INSERT INTO users VALUES (2, 'Bob', 'bob@test.com');
Success: 1 row inserted
```

### Querying Data

Simple SELECT queries:

```
bayundb> SELECT * FROM users;
| id | name    | email           |
+----+---------+-----------------+
| 1  | "Alice" | "alice@test.com" |
| 2  | "Bob"   | "bob@test.com"   |
(2 rows)
```

Filtered queries:

```
bayundb> SELECT name, email FROM users WHERE id = 1;
| name    | email           |
+---------+-----------------+
| "Alice" | "alice@test.com" |
(1 row)
```

### Updating Data

```
bayundb> UPDATE users SET email = 'alice.new@test.com' WHERE id = 1;
Success: 1 row updated
```

### Deleting Data

```
bayundb> DELETE FROM users WHERE id = 2;
Success: 1 row deleted
```

## Advanced Usage

### Using Helper Scripts

For convenience, BayunDB provides helper scripts:

On Unix/Linux/macOS:
```bash
./scripts/run_cli.sh query "SELECT * FROM users"
```

On Windows:
```
scripts\run_cli.bat query "SELECT * FROM users"
```

### Input Redirection

You can execute a series of SQL commands from a file:

```bash
# Create a SQL file
echo "SELECT * FROM users;" > commands.sql
echo "help" >> commands.sql
echo "exit" >> commands.sql

# Execute the commands
cat commands.sql | ./target/debug/bnql
```

### Adjusting Buffer Pool Size

For performance tuning, you can adjust the buffer pool size:

```bash
./target/debug/bnql --buffer-size 2000
```

This sets the buffer pool to cache up to 2000 pages in memory.

## Command Reference

| Command | Description | Example |
|---------|-------------|---------|
| `help` | Display available commands | `bayundb> help` |
| `exit`, `quit` | Exit the CLI | `bayundb> exit` |
| Any SQL statement | Execute SQL directly | `bayundb> SELECT * FROM users` |

## Troubleshooting

### Common Issues

1. **Database file not found**: Ensure the specified database file exists or use the `create` command to create it.
   
2. **Permission errors**: Make sure you have read/write permissions for the database file and log directory.

3. **Syntax errors**: Check your SQL syntax if you receive a parser error.

### Getting Help

If you encounter issues, try running with the `--help` flag for available options:

```bash
./target/debug/bnql --help
```

## Next Steps

- Learn how to write complex queries in the [Advanced SQL Tutorial](advanced_sql.md)
- Explore database performance tuning in the [Performance Guide](../reference/performance.md)
- Set up a production environment with the [Deployment Guide](../reference/deployment.md) 