# BayunDB Command-Line Interface

The BayunDB Command-Line Interface (CLI) provides a user-friendly way to interact with the database directly from the terminal. It allows users to execute SQL queries, manage database files, and explore database information without writing code.

## Overview

The CLI offers a fully-featured interactive SQL shell with command history, as well as direct command execution for scripting and automation. It integrates with BayunDB's core features, including the query engine, transaction management, and storage components.

## Implementation

The CLI is implemented in `src/bin/cli.rs` and follows a modular architecture:

- **Command-line argument parsing**: Using the `clap` library for a structured and user-friendly interface
- **Interactive shell**: Powered by `rustyline` for enhanced editing capabilities and command history
- **Query execution**: Direct integration with BayunDB's query execution engine
- **Output formatting**: Custom result formatting for readable table output

## Features

### Interactive Shell

The interactive shell provides a REPL (Read-Eval-Print Loop) environment for executing SQL queries and commands:

```
$ bnql
Welcome to BayunDB CLI. Type 'help' for assistance or 'exit' to quit.
bayundb> SELECT * FROM users;
| id | name    | email           |
+----+---------+-----------------+
| 1  | "Alice" | "alice@test.com" |
| 2  | "Bob"   | "bob@test.com"   |
(2 rows)
bayundb> help
Available commands:
  CREATE TABLE <name> (...)     - Create a new table
  INSERT INTO <table> VALUES    - Insert data into a table
  SELECT ... FROM <table>       - Query data from a table
  UPDATE <table> SET ...        - Update data in a table
  DELETE FROM <table>           - Delete data from a table
  help                          - Display this help message
  exit                          - Exit the CLI
bayundb> exit
Goodbye!
```

### Direct Query Execution

For scripting and automation, the CLI supports direct query execution:

```
$ bnql query "SELECT * FROM users WHERE id = 1"
| id | name    | email           |
+----+---------+-----------------+
| 1  | "Alice" | "alice@test.com" |
(1 row)
```

### Database Information

The CLI provides database metadata and configuration information:

```
$ bnql info
BayunDB Information:
  Database file: database.db
  Log directory: logs
  Buffer pool size: 1000 pages
```

### Database Creation

You can initialize a new database directly from the CLI:

```
$ bnql create
Creating new database at: database.db
Database created successfully!
```

## Command-Line Options

The CLI provides the following command-line options:

| Option | Description | Default |
|--------|-------------|---------|
| `--db-path <path>` | Path to the database file | `database.db` |
| `--log-dir <path>` | Directory for WAL log files | `logs` |
| `--buffer-size <size>` | Buffer pool size in pages | 1000 |

## Commands

The CLI supports the following subcommands:

| Command | Description |
|---------|-------------|
| `shell` | Start an interactive SQL shell (default if no command specified) |
| `query <sql>` | Execute a SQL query and display results |
| `create` | Create a new database |
| `info` | Display database information |

## Helper Scripts

For convenience, the following helper scripts are provided:

- `scripts/run_cli.bat` (Windows)
- `scripts/run_cli.sh` (Unix/Linux/macOS)

These scripts build and run the CLI in a single step:

```
$ ./scripts/run_cli.sh query "SELECT * FROM users"
```

## Testing

The CLI includes both unit tests and integration tests:

- **Unit tests**: Testing individual CLI components in isolation (result formatting, help text)
- **Integration tests**: End-to-end tests of CLI functionality (command execution, input/output handling)

### Running Tests

```
# Run all CLI tests
cargo test --bin bnql

# Run integration tests
cargo test --test cli_test
```

## Future Improvements

The CLI will continue to evolve alongside the database. Planned enhancements include:

- Support for loading and executing SQL script files
- Enhanced result export (CSV, JSON)
- Visual query plan visualization
- Database statistics and monitoring
- Tab completion for SQL keywords and table/column names
- Automatic schema discovery for suggestions 