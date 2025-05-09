use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use std::sync::Arc;
use std::time::Duration;

use bayundb::query::parser::Parser;
use bayundb::query::planner::QueryPlanner;
use bayundb::query::executor::QueryExecutor;
use bayundb::storage::buffer::BufferPoolManager;
use bayundb::catalog::Catalog;

// Create a test environment with tables populated with test data
fn setup_test_environment(buffer_pool_size: usize) -> (Arc<BufferPoolManager>, Arc<Catalog>) {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    let buffer_pool = Arc::new(BufferPoolManager::new(buffer_pool_size, path).unwrap());
    
    // Keep the temp file alive
    std::mem::forget(temp_file);
    
    // Initialize catalog
    let catalog = Arc::new(Catalog::new(buffer_pool.clone()).unwrap());
    
    // Create test tables and populate with data
    // This would involve creating users, orders, and products tables with sample data
    // Code to create and populate tables would go here
    
    (buffer_pool, catalog)
}

fn query_execution_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("QueryExecution");
    
    // Configure benchmarks
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);
    
    // Simple queries of different complexity
    let queries = [
        "SELECT id, name FROM users WHERE id > 100",
        "SELECT * FROM products WHERE price < 50.0",
        "SELECT u.id, u.name, o.order_id FROM users u JOIN orders o ON u.id = o.user_id",
        "SELECT product_id, COUNT(*) FROM orders GROUP BY product_id",
    ];
    
    for (i, query) in queries.iter().enumerate() {
        group.bench_with_input(BenchmarkId::new("execute_query", i), query, |b, query| {
            let (buffer_pool, catalog) = setup_test_environment(1000);
            let executor = QueryExecutor::new(catalog.clone(), buffer_pool.clone());
            
            b.iter(|| {
                // Parse query
                let mut parser = Parser::new(query);
                let statement = parser.parse_statement().unwrap();
                
                // Plan query
                let planner = QueryPlanner::new(catalog.clone());
                let plan = planner.create_plan(&statement).unwrap();
                
                // Execute query and collect results
                let result = executor.execute_query(plan).unwrap();
                let _ = result.collect::<Vec<_>>();
            });
        });
    }
    
    // Benchmark different join types/algorithms
    let join_queries = [
        // Nested loop join
        "SELECT u.id, o.order_id FROM users u, orders o WHERE u.id = o.user_id",
        // Hash join (if implemented)
        "SELECT u.id, o.order_id FROM users u JOIN orders o ON u.id = o.user_id",
        // Multi-table join
        "SELECT u.name, p.name, o.quantity FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id",
    ];
    
    for (i, query) in join_queries.iter().enumerate() {
        group.bench_with_input(BenchmarkId::new("join_execution", i), query, |b, query| {
            let (buffer_pool, catalog) = setup_test_environment(1000);
            let executor = QueryExecutor::new(catalog.clone(), buffer_pool.clone());
            
            b.iter(|| {
                let mut parser = Parser::new(query);
                let statement = parser.parse_statement().unwrap();
                
                let planner = QueryPlanner::new(catalog.clone());
                let plan = planner.create_plan(&statement).unwrap();
                
                let result = executor.execute_query(plan).unwrap();
                let _ = result.collect::<Vec<_>>();
            });
        });
    }
    
    // Aggregation queries benchmark
    let agg_queries = [
        "SELECT COUNT(*) FROM orders",
        "SELECT product_id, SUM(quantity) FROM orders GROUP BY product_id",
        "SELECT u.id, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id",
    ];
    
    for (i, query) in agg_queries.iter().enumerate() {
        group.bench_with_input(BenchmarkId::new("aggregation", i), query, |b, query| {
            let (buffer_pool, catalog) = setup_test_environment(1000);
            let executor = QueryExecutor::new(catalog.clone(), buffer_pool.clone());
            
            b.iter(|| {
                let mut parser = Parser::new(query);
                let statement = parser.parse_statement().unwrap();
                
                let planner = QueryPlanner::new(catalog.clone());
                let plan = planner.create_plan(&statement).unwrap();
                
                let result = executor.execute_query(plan).unwrap();
                let _ = result.collect::<Vec<_>>();
            });
        });
    }
    
    group.finish();
}

criterion_group!(benches, query_execution_benchmark);
criterion_main!(benches); 