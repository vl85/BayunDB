use std::time::Duration;
use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use rusqlite::{Connection as SqliteConnection, Result as SqliteResult};
use bayundb::query::executor::engine::ExecutionEngine;
use bayundb::storage::buffer::BufferPoolManager;
use bayundb::catalog::Catalog;
use bayundb::transaction::concurrency::transaction_manager::TransactionManager;
use bayundb::transaction::wal::log_manager::{LogManager, LogManagerConfig};
use bayundb::transaction::wal::log_buffer::LogBufferConfig;
use std::sync::{Arc, RwLock};
use tempfile::NamedTempFile;
use std::path::PathBuf;

use super::{BenchmarkResult, BenchmarkReport, get_system_info, write_results_to_json, generate_markdown_report};

// Simplified embedded SQLite API for benchmarking
struct EmbeddedSqlite {
    connection: SqliteConnection,
    _temp_file: NamedTempFile, // Keep temp file alive while the connection exists
}

impl EmbeddedSqlite {
    fn new() -> Self {
        let temp_file = NamedTempFile::new().unwrap();
        let connection = SqliteConnection::open(temp_file.path()).unwrap();
        
        EmbeddedSqlite {
            connection,
            _temp_file: temp_file,
        }
    }
    
    fn execute_query(&self, sql: &str) -> SqliteResult<usize> {
        self.connection.execute(sql, [])
    }
    
    fn query_scalar<T: rusqlite::types::FromSql>(&self, sql: &str) -> SqliteResult<T> {
        self.connection.query_row(sql, [], |row| row.get(0))
    }
}

// Simplified embedded BayunDB API for benchmarking
struct EmbeddedBayunDb {
    engine: ExecutionEngine,
    _buffer_pool: Arc<BufferPoolManager>,
    _catalog: Arc<RwLock<Catalog>>,
    _transaction_manager: Arc<TransactionManager>,
    _temp_file: NamedTempFile, // Keep temp file alive while the connection exists
}

impl EmbeddedBayunDb {
    fn new() -> Self {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();
        
        let buffer_pool = Arc::new(BufferPoolManager::new(1000, &path).unwrap());
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        
        let wal_dir = temp_file.path().parent().unwrap_or_else(|| std::path::Path::new("."));
        let log_file_base_name = temp_file.path()
            .file_stem()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        
        let log_manager_config = LogManagerConfig {
            log_dir: wal_dir.to_path_buf(),
            log_file_base_name,
            max_log_file_size: 10 * 1024 * 1024,
            buffer_config: LogBufferConfig::default(),
            force_sync: false,
        };
        
        let log_manager = Arc::new(LogManager::new(log_manager_config).unwrap());
        let transaction_manager = Arc::new(TransactionManager::new(log_manager));
        
        let engine = ExecutionEngine::new(
            buffer_pool.clone(),
            catalog.clone(),
            transaction_manager.clone(),
        );
        
        EmbeddedBayunDb {
            engine,
            _buffer_pool: buffer_pool,
            _catalog: catalog,
            _transaction_manager: transaction_manager,
            _temp_file: temp_file,
        }
    }
    
    fn execute_query(&self, sql: &str) -> anyhow::Result<usize> {
        let result = self.engine.execute_query(sql)?;
        Ok(result.row_count())
    }
}

fn benchmark_embedded_api(c: &mut Criterion) {
    let mut group = c.benchmark_group("EmbeddedAPI");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);
    
    // Operations to benchmark
    let operations = vec![
        ("Database Creation", None),
        ("Table Creation", Some("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")),
        ("Data Insertion", Some("INSERT INTO users VALUES (1, 'User 1', 'user1@example.com')")),
        ("Simple Query", Some("SELECT * FROM users WHERE id = 1")),
    ];
    
    for (operation_name, sql) in operations {
        // SQLite benchmark
        group.bench_with_input(
            BenchmarkId::new("SQLite", operation_name),
            &operation_name,
            |b, _| {
                match operation_name {
                    "Database Creation" => {
                        // Benchmark database creation
                        b.iter(|| {
                            let _db = EmbeddedSqlite::new();
                            1 // Return rows affected
                        });
                    },
                    _ => {
                        // For all other operations, set up the database first
                        let db = EmbeddedSqlite::new();
                        if operation_name == "Data Insertion" || operation_name == "Simple Query" {
                            // Create table if needed for insertion or query
                            db.execute_query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)").unwrap();
                        }
                        
                        if operation_name == "Simple Query" {
                            // Insert data if we're testing queries
                            db.execute_query("INSERT INTO users VALUES (1, 'User 1', 'user1@example.com')").unwrap();
                        }
                        
                        // Now benchmark the actual operation
                        b.iter(|| {
                            let sql = sql.unwrap();
                            db.execute_query(sql).unwrap_or(0)
                        });
                    }
                }
            },
        );
        
        // BayunDB benchmark
        group.bench_with_input(
            BenchmarkId::new("BayunDB", operation_name),
            &operation_name,
            |b, _| {
                match operation_name {
                    "Database Creation" => {
                        // Benchmark database creation
                        b.iter(|| {
                            let _db = EmbeddedBayunDb::new();
                            1 // Return rows affected
                        });
                    },
                    _ => {
                        // For all other operations, set up the database first
                        let db = EmbeddedBayunDb::new();
                        if operation_name == "Data Insertion" || operation_name == "Simple Query" {
                            // Create table if needed for insertion or query
                            db.execute_query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)").unwrap();
                        }
                        
                        if operation_name == "Simple Query" {
                            // Insert data if we're testing queries
                            db.execute_query("INSERT INTO users VALUES (1, 'User 1', 'user1@example.com')").unwrap();
                        }
                        
                        // Now benchmark the actual operation
                        b.iter(|| {
                            let sql = sql.unwrap();
                            db.execute_query(sql).unwrap_or(0)
                        });
                    }
                }
            },
        );
    }
    
    group.finish();
}

criterion_group!(benches, benchmark_embedded_api);
criterion_main!(benches); 