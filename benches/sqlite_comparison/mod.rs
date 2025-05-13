use std::time::{Duration, Instant};
use std::fs;
use std::path::Path;
use serde::{Serialize, Deserialize};
use criterion::{Criterion, BenchmarkId};
use rusqlite::Connection as SqliteConnection;
use bayundb::query::executor::engine::ExecutionEngine;
use bayundb::storage::buffer::BufferPoolManager;
use bayundb::catalog::Catalog;
use bayundb::transaction::concurrency::transaction_manager::TransactionManager;
use bayundb::transaction::wal::log_manager::{LogManager, LogManagerConfig};
use bayundb::transaction::wal::log_buffer::LogBufferConfig;
use std::sync::{Arc, RwLock};
use std::path::PathBuf;
use tempfile::TempDir;

/// Benchmark result for a single operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    /// Type of operation being benchmarked
    pub operation: String,
    /// Database engine ("SQLite" or "BayunDB")
    pub database: String,
    /// Number of rows affected by the operation
    pub rows_affected: usize,
    /// Execution time in milliseconds
    pub execution_time_ms: f64,
    /// Memory usage in kilobytes (if available)
    pub memory_usage_kb: Option<usize>,
    /// Extra metrics specific to the benchmark
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra_metrics: Option<serde_json::Value>,
}

/// Collection of benchmark results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkReport {
    /// Name of the benchmark suite
    pub name: String,
    /// Timestamp when benchmark was run
    pub timestamp: String,
    /// System information
    pub system_info: SystemInfo,
    /// Collection of benchmark results
    pub results: Vec<BenchmarkResult>,
}

/// System information for reproducibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    /// Operating system
    pub os: String,
    /// CPU information
    pub cpu: String,
    /// Memory information
    pub memory: String,
    /// Rust version
    pub rust_version: String,
    /// SQLite version
    pub sqlite_version: String,
    /// BayunDB version
    pub bayundb_version: String,
}

/// Benchmark environment for SQLite
pub struct SqliteBenchmarkEnv {
    /// Temporary directory for SQLite database
    pub temp_dir: TempDir,
    /// SQLite connection
    pub connection: SqliteConnection,
    /// Path to SQLite database file
    pub db_path: PathBuf,
}

/// Benchmark environment for BayunDB
pub struct BayunDbBenchmarkEnv {
    /// Temporary directory for BayunDB database
    pub temp_dir: TempDir,
    /// BayunDB buffer pool
    pub buffer_pool: Arc<BufferPoolManager>,
    /// BayunDB catalog
    pub catalog: Arc<RwLock<Catalog>>,
    /// BayunDB transaction manager
    pub transaction_manager: Arc<TransactionManager>,
    /// BayunDB execution engine
    pub engine: ExecutionEngine,
    /// Path to BayunDB database file
    pub db_path: PathBuf,
}

impl SqliteBenchmarkEnv {
    /// Create a new SQLite benchmark environment
    pub fn new() -> Self {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("sqlite_benchmark.db");
        let connection = SqliteConnection::open(&db_path).unwrap();
        
        // Enable foreign keys and other configurations
        connection.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        
        SqliteBenchmarkEnv {
            temp_dir,
            connection,
            db_path,
        }
    }
    
    /// Execute an SQL query and return affected rows
    pub fn execute(&self, sql: &str) -> rusqlite::Result<usize> {
        self.connection.execute(sql, [])
    }
    
    /// Execute an SQL query and return rows
    pub fn query<T, F>(&self, sql: &str, mut row_callback: F) -> rusqlite::Result<Vec<T>>
    where
        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        let mut stmt = self.connection.prepare(sql)?;
        let rows = stmt.query_map([], |row| row_callback(row))?;
        
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        
        Ok(results)
    }
}

impl BayunDbBenchmarkEnv {
    /// Create a new BayunDB benchmark environment
    pub fn new() -> Self {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("bayundb_benchmark.db");
        let db_path_str = db_path.to_str().unwrap().to_string();
        
        // Create buffer pool
        let buffer_pool = Arc::new(BufferPoolManager::new(1000, &db_path_str).unwrap());
        
        // Create catalog
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        
        // Create log manager
        let wal_dir = temp_dir.path().to_path_buf();
        let log_file_base_name = "benchmark_wal".to_string();
        let log_manager_config = LogManagerConfig {
            log_dir: wal_dir,
            log_file_base_name,
            max_log_file_size: 10 * 1024 * 1024, // 10MB
            buffer_config: LogBufferConfig::default(),
            force_sync: false,
        };
        let log_manager = Arc::new(LogManager::new(log_manager_config).unwrap());
        
        // Create transaction manager
        let transaction_manager = Arc::new(TransactionManager::new(log_manager));
        
        // Create execution engine
        let engine = ExecutionEngine::new(
            buffer_pool.clone(),
            catalog.clone(),
            transaction_manager.clone(),
        );
        
        BayunDbBenchmarkEnv {
            temp_dir,
            buffer_pool,
            catalog,
            transaction_manager,
            engine,
            db_path,
        }
    }
    
    /// Execute an SQL query and return result
    pub fn execute(&self, sql: &str) -> anyhow::Result<bayundb::query::executor::result::QueryResult> {
        self.engine.execute_query(sql)
    }
}

/// Benchmark a single operation
pub fn benchmark_operation<F>(operation_name: &str, database_name: &str, operation: F) -> BenchmarkResult
where
    F: FnOnce() -> usize,
{
    let start = Instant::now();
    let rows_affected = operation();
    let duration = start.elapsed();
    
    BenchmarkResult {
        operation: operation_name.to_string(),
        database: database_name.to_string(),
        rows_affected,
        execution_time_ms: duration.as_secs_f64() * 1000.0,
        memory_usage_kb: None, // Memory tracking could be added with a crate like memory-stats
        extra_metrics: None,
    }
}

/// Write benchmark results to a JSON file
pub fn write_results_to_json(report: &BenchmarkReport, output_path: &Path) -> std::io::Result<()> {
    let json = serde_json::to_string_pretty(report)?;
    fs::write(output_path, json)?;
    Ok(())
}

/// Generate a markdown report from benchmark results
pub fn generate_markdown_report(report: &BenchmarkReport, output_path: &Path) -> std::io::Result<()> {
    let mut markdown = format!("# {} Benchmark Report\n\n", report.name);
    markdown.push_str(&format!("Generated: {}\n\n", report.timestamp));
    
    // System info
    markdown.push_str("## System Information\n\n");
    markdown.push_str(&format!("- OS: {}\n", report.system_info.os));
    markdown.push_str(&format!("- CPU: {}\n", report.system_info.cpu));
    markdown.push_str(&format!("- Memory: {}\n", report.system_info.memory));
    markdown.push_str(&format!("- Rust: {}\n", report.system_info.rust_version));
    markdown.push_str(&format!("- SQLite: {}\n", report.system_info.sqlite_version));
    markdown.push_str(&format!("- BayunDB: {}\n\n", report.system_info.bayundb_version));
    
    // Group results by operation
    let mut operation_groups: std::collections::HashMap<String, Vec<&BenchmarkResult>> = std::collections::HashMap::new();
    
    for result in &report.results {
        operation_groups.entry(result.operation.clone())
            .or_insert_with(Vec::new)
            .push(result);
    }
    
    // Generate tables for each operation group
    for (operation, results) in operation_groups {
        markdown.push_str(&format!("## {} Performance\n\n", operation));
        markdown.push_str("| Database | Rows Affected | Time (ms) | Ratio to SQLite |\n");
        markdown.push_str("|----------|---------------|-----------|----------------|\n");
        
        // Find SQLite result to calculate ratio
        let sqlite_result = results.iter().find(|r| r.database == "SQLite");
        let sqlite_time = sqlite_result.map(|r| r.execution_time_ms).unwrap_or(0.0);
        
        for result in results {
            let ratio = if sqlite_time > 0.0 && result.database != "SQLite" {
                format!("{:.2}x", result.execution_time_ms / sqlite_time)
            } else {
                "1.00x".to_string()
            };
            
            markdown.push_str(&format!(
                "| {} | {} | {:.2} | {} |\n",
                result.database,
                result.rows_affected,
                result.execution_time_ms,
                ratio
            ));
        }
        
        markdown.push_str("\n");
    }
    
    fs::write(output_path, markdown)?;
    Ok(())
}

/// Get system information
pub fn get_system_info() -> SystemInfo {
    SystemInfo {
        os: std::env::consts::OS.to_string(),
        cpu: std::env::var("PROCESSOR_IDENTIFIER").unwrap_or_else(|_| "Unknown".to_string()),
        memory: "Unknown".to_string(), // Could use system-info crate for better detection
        rust_version: rustc_version_runtime::version().to_string(),
        sqlite_version: rusqlite::version().to_string(),
        bayundb_version: env!("CARGO_PKG_VERSION").to_string(),
    }
}

// Re-export key dependencies for benchmarks
pub use criterion;
pub use rusqlite;
pub use serde;
pub use serde_json; 