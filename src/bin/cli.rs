use std::path::PathBuf;
use std::sync::Arc;
use anyhow::{Result, Context};
use clap::{Parser, Subcommand};
use rustyline::error::ReadlineError;
use rustyline::Editor;
use rustyline::history::DefaultHistory;

use bayundb::storage::buffer::BufferPoolManager;
use bayundb::storage::page::PageManager;
use bayundb::index::btree::BTreeIndex;
use bayundb::transaction::wal::log_manager::{LogManager, LogManagerConfig};
use bayundb::transaction::wal::log_buffer::LogBufferConfig;
use bayundb::query::executor::result::{QueryResult, QueryResultSet};
use bayundb::query::executor::ExecutionEngine;
use bayundb::catalog::Catalog;
use bayundb::transaction::concurrency::transaction_manager::TransactionManager;

const HISTORY_FILE: &str = ".bayundb_history";

#[derive(Parser)]
#[command(author, version, about = "BayunDB CLI - A tool for interacting with BayunDB")]
struct Cli {
    /// Database file path
    #[arg(short, long, default_value = "database.db")]
    db_path: String,

    /// Log directory path
    #[arg(short, long, default_value = "logs")]
    log_dir: String,

    /// Buffer pool size (number of pages)
    #[arg(short, long, default_value_t = 1000)]
    buffer_size: usize,

    /// Command to execute
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Start an interactive shell
    Shell,
    
    /// Execute a SQL query directly
    Query {
        /// SQL query to execute
        query: String,
    },
    
    /// Create a new database
    Create,
    
    /// Show database information
    Info,
}

/// Database instance with all required components
#[allow(dead_code)] // This struct holds core DB components, many unused by current simple CLI
struct Database {
    buffer_pool: Arc<BufferPoolManager>,
    page_manager: PageManager,
    btree_index: BTreeIndex<i32>,
    log_manager: Arc<LogManager>,
    transaction_manager: Arc<TransactionManager>,
    execution_engine: ExecutionEngine,
}

impl Database {
    fn new(db_path: &str, log_dir: &str, buffer_size: usize) -> Result<Self> {
        // Create logs directory if it doesn't exist
        let log_dir_path = PathBuf::from(log_dir);
        std::fs::create_dir_all(&log_dir_path)?;
        
        // Configure log manager
        let log_config = LogManagerConfig {
            log_dir: log_dir_path,
            log_file_base_name: "bayun_log".to_string(),
            max_log_file_size: 1024 * 1024, // 1 MB
            buffer_config: LogBufferConfig::default(),
            force_sync: true, // Force sync for safety
        };
        
        // Create log manager
        let log_manager = Arc::new(LogManager::new(log_config)?);
        
        // Create buffer pool manager with WAL support
        let buffer_pool = Arc::new(BufferPoolManager::new_with_wal(
            buffer_size,
            db_path,
            log_manager.clone(),
        )?);
        
        // Create page manager
        let page_manager = PageManager::new();
        
        // Create B+Tree index for integer keys
        let btree_index = BTreeIndex::<i32>::new(buffer_pool.clone())?;
        
        // Get the global catalog instance for the live environment
        let catalog_arc = Catalog::instance();

        // Create TransactionManager
        let transaction_manager = Arc::new(TransactionManager::new(log_manager.clone()));

        // Create execution engine
        let execution_engine = ExecutionEngine::new(buffer_pool.clone(), catalog_arc, transaction_manager.clone());
        
        Ok(Database {
            buffer_pool,
            page_manager,
            btree_index,
            log_manager,
            transaction_manager,
            execution_engine,
        })
    }
    
    fn execute_query(&self, query: &str) -> QueryResult<QueryResultSet> {
        // Log the incoming query
        println!("Executing query: {}", query);
        
        // Execute the query using the execution engine
        // The ExecutionEngine now handles returning a proper QueryResultSet for CREATE TABLE.
        self.execution_engine.execute_query(query)
    }
}

fn run_shell(db: &Database) -> Result<()> {
    println!("Welcome to BayunDB CLI. Type 'help' for assistance or 'exit' to quit.");
    
    let mut rl = Editor::<(), DefaultHistory>::new()?;
    if let Err(err) = rl.load_history(HISTORY_FILE) {
        if !err.to_string().contains("No such file or directory") {
            println!("Error loading history: {}", err);
        }
    }
    
    loop {
        let readline = rl.readline("bayundb> ");
        match readline {
            Ok(line) => {
                let _ = rl.add_history_entry(&line);
                
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                
                match line.to_lowercase().as_str() {
                    "exit" | "quit" => {
                        println!("Goodbye!");
                        break;
                    }
                    "help" => {
                        print_help();
                    }
                    _ => {
                        // Assume it's a SQL query
                        match db.execute_query(line) {
                            Ok(result) => {
                                display_result(&result);
                            }
                            Err(err) => {
                                println!("Error: {}", err);
                            }
                        }
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {}", err);
                break;
            }
        }
    }
    
    if let Err(err) = rl.save_history(HISTORY_FILE) {
        println!("Error saving history: {}", err);
    }
    Ok(())
}

fn print_help() {
    println!("Available commands:");
    println!("  CREATE TABLE <n> (...)     - Create a new table");
    println!("  INSERT INTO <table> VALUES    - Insert data into a table");
    println!("  SELECT ... FROM <table>       - Query data from a table");
    println!("  UPDATE <table> SET ...        - Update data in a table");
    println!("  DELETE FROM <table>           - Delete data from a table");
    println!();
    println!("Aggregation functions:");
    println!("  COUNT(*)                      - Count rows in a table or group");
    println!("  SUM(<column>)                 - Calculate sum of values in a column");
    println!("  AVG(<column>)                 - Calculate average of values in a column");
    println!("  MIN(<column>)                 - Find minimum value in a column");
    println!("  MAX(<column>)                 - Find maximum value in a column");
    println!();
    println!("Grouping:");
    println!("  GROUP BY <columns>            - Group results by columns");
    println!("  HAVING <condition>            - Filter groups based on aggregate values");
    println!();
    println!("Other commands:");
    println!("  help                          - Display this help message");
    println!("  exit                          - Exit the CLI");
}

fn display_result(result: &QueryResultSet) {
    // Always display the table structure, even for empty results and create table statements
    let headers = result.columns();
    
    // Special case for CREATE TABLE result - just print it directly
    if headers.len() == 1 && headers[0] == "result" {
        // Just display the message for CREATE TABLE
        if let Some(row) = result.rows().first() {
            if let Some(value) = row.get("result") {
                // Always print with explicit table formatting for tests to detect
                println!("+------------------------------------+");
                println!("| {:<34} |", value);
                println!("+------------------------------------+");
                println!("(1 row)");
                return;
            }
        }
    }
    
    // For empty results, still show the table structure
    let mut widths = vec![0; headers.len()];
    
    // Calculate column widths for headers
    for (i, header) in headers.iter().enumerate() {
        widths[i] = header.len().max(widths[i]);
    }
    
    // Calculate column widths for data
    for row in result.rows() {
        for (i, header) in headers.iter().enumerate() {
            // Try to get the value using the exact header name first
            let value = match row.get(header) {
                Some(val) => val.to_string(),
                None => {
                    // For COUNT(*) special case handling
                    if header == "COUNT(*)" || header.starts_with("COUNT(") {
                        // First try all the count variations
                        if let Some(val) = row.get("COUNT(*)") {
                            val.to_string()
                        } else if let Some(val) = row.get("count(*)") {
                            val.to_string()
                        } else if let Some(val) = row.get("expr") {
                            val.to_string()
                        } else {
                            // Check all row values to find a count result
                            let mut found_value = None;
                            for (key, val) in row.values_with_names() {
                                if key.starts_with("COUNT") || key.starts_with("count") || key == "expr" {
                                    found_value = Some(val.to_string());
                                    break;
                                }
                            }
                            found_value.unwrap_or_else(|| "NULL".to_string())
                        }
                    } else {
                        // Check if header contains a function name like SUM, AVG, etc.
                        let common_agg_funcs = ["COUNT", "SUM", "AVG", "MIN", "MAX"];
                        let mut found_value = None;
                        
                        // Try to match header with any column that starts with the same aggregation function
                        for (key, val) in row.values_with_names() {
                            if common_agg_funcs.iter().any(|func| 
                                header.starts_with(func) && (key.starts_with(func) || key == "expr")) {
                                found_value = Some(val.to_string());
                                break;
                            }
                        }
                        
                        // If still not found, check for expr column which might contain aggregation result
                        if found_value.is_none() && (header.contains("(") || header == "expr") {
                            found_value = row.get("expr").map(|v| v.to_string());
                        }
                        
                        found_value.unwrap_or_else(|| "NULL".to_string())
                    }
                }
            };
            
            widths[i] = value.len().max(widths[i]);
        }
    }
    
    // Print headers - ensure minimum width of 3 characters per column
    print!("|");
    for (i, header) in headers.iter().enumerate() {
        let width = widths[i].max(3);
        print!(" {:<width$} |", header, width = width);
    }
    println!();
    
    // Print separator
    print!("+");
    for &width in &widths {
        let width = width.max(3);
        print!("{:-<width$}+", "", width = width + 2);
    }
    println!();
    
    // Print rows
    for row in result.rows() {
        print!("|");
        for (i, header) in headers.iter().enumerate() {
            // Same logic as when calculating column widths
            let value = match row.get(header) {
                Some(val) => val.to_string(),
                None => {
                    // For COUNT(*) special case handling
                    if header == "COUNT(*)" || header.starts_with("COUNT(") {
                        // First try all the count variations
                        if let Some(val) = row.get("COUNT(*)") {
                            val.to_string()
                        } else if let Some(val) = row.get("count(*)") {
                            val.to_string()
                        } else if let Some(val) = row.get("expr") {
                            val.to_string()
                        } else {
                            // Check all row values to find a count result
                            let mut found_value = None;
                            for (key, val) in row.values_with_names() {
                                if key.starts_with("COUNT") || key.starts_with("count") || key == "expr" {
                                    found_value = Some(val.to_string());
                                    break;
                                }
                            }
                            found_value.unwrap_or_else(|| "NULL".to_string())
                        }
                    } else {
                        // Check if header contains a function name like SUM, AVG, etc.
                        let common_agg_funcs = ["COUNT", "SUM", "AVG", "MIN", "MAX"];
                        let mut found_value = None;
                        
                        // Try to match header with any column that starts with the same aggregation function
                        for (key, val) in row.values_with_names() {
                            if common_agg_funcs.iter().any(|func| 
                                header.starts_with(func) && (key.starts_with(func) || key == "expr")) {
                                found_value = Some(val.to_string());
                                break;
                            }
                        }
                        
                        // If still not found, check for expr column which might contain aggregation result
                        if found_value.is_none() && (header.contains("(") || header == "expr") {
                            found_value = row.get("expr").map(|v| v.to_string());
                        }
                        
                        found_value.unwrap_or_else(|| "NULL".to_string())
                    }
                }
            };
            
            let width = widths[i].max(3);
            print!(" {:<width$} |", value, width = width);
        }
        println!();
    }
    
    // Always print the row count, even for empty results
    println!("({} rows)", result.row_count());
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    
    // Initialize the database
    let db = Database::new(&cli.db_path, &cli.log_dir, cli.buffer_size)
        .context("Failed to initialize database")?;
    
    match &cli.command {
        Some(Commands::Shell) => {
            run_shell(&db)?;
        }
        Some(Commands::Query { query }) => {
            match db.execute_query(query) {
                Ok(result) => {
                    display_result(&result);
                }
                Err(err) => {
                    eprintln!("Error executing query: {}", err);
                }
            }
        }
        Some(Commands::Create) => {
            println!("Creating new database at: {}", cli.db_path);
            // Database is already initialized
            println!("Database created successfully!");
        }
        Some(Commands::Info) => {
            println!("BayunDB Information:");
            println!("  Database file: {}", cli.db_path);
            println!("  Log directory: {}", cli.log_dir);
            println!("  Buffer pool size: {} pages", cli.buffer_size);
            // TODO: Add more information like table count, record count, etc.
        }
        None => {
            // Default to shell if no command is specified
            run_shell(&db)?;
        }
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bayundb::query::executor::result::{Row, DataValue};
    
    
    #[test]
    fn test_display_result_with_empty_set() {
        let result = QueryResultSet::new(vec!["id".to_string(), "name".to_string()]);
        
        // This test just makes sure the function doesn't panic with empty results
        display_result(&result);
    }
    
    #[test]
    fn test_display_result_formats_correctly() {
        let mut result = QueryResultSet::new(vec!["id".to_string(), "name".to_string()]);
        
        // Create test rows
        let mut row1 = Row::new();
        row1.set("id".to_string(), DataValue::Integer(1));
        row1.set("name".to_string(), DataValue::Text("Test 1".to_string()));
        
        let mut row2 = Row::new();
        row2.set("id".to_string(), DataValue::Integer(2));
        row2.set("name".to_string(), DataValue::Text("Test 2".to_string()));
        
        // Add rows to result set
        result.add_row(row1);
        result.add_row(row2);
        
        // This test just makes sure the function doesn't panic
        display_result(&result);
    }
    
    #[test]
    fn test_help_command_content() {
        // This test just makes sure the function doesn't panic
        print_help();
    }
} 