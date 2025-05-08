use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use std::time::Duration;

use bayundb::query::parser::Parser;
use bayundb::query::parser::ast::Statement;

fn query_parser_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("QueryParser");
    
    // Configure the benchmarks
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(100);
    
    // Benchmark simple SELECT queries
    let simple_queries = [
        "SELECT id, name FROM users WHERE id > 100",
        "SELECT * FROM products WHERE price < 50.0 AND category = 'electronics'",
        "SELECT id, title, description FROM articles WHERE published_date > '2023-01-01'",
    ];
    
    for (i, query) in simple_queries.iter().enumerate() {
        group.bench_with_input(BenchmarkId::new("simple_select", i), query, |b, query| {
            b.iter(|| {
                let mut parser = Parser::new(query);
                let _ = parser.parse_statement().unwrap();
            });
        });
    }
    
    // Benchmark queries with JOIN operations
    let join_queries = [
        // INNER JOIN
        "SELECT u.id, u.name, o.order_id FROM users u JOIN orders o ON u.id = o.user_id",
        // LEFT JOIN
        "SELECT u.id, u.name, o.order_id FROM users u LEFT JOIN orders o ON u.id = o.user_id",
        // Multiple JOINs
        "SELECT u.id, u.name, o.order_id, i.item_name FROM users u JOIN orders o ON u.id = o.user_id JOIN items i ON o.item_id = i.id",
        // Complex JOIN with filtering
        "SELECT u.id, u.name, o.order_id FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 100 AND u.status = 'active'",
    ];
    
    for (i, query) in join_queries.iter().enumerate() {
        group.bench_with_input(BenchmarkId::new("join_query", i), query, |b, query| {
            b.iter(|| {
                let mut parser = Parser::new(query);
                let _ = parser.parse_statement().unwrap();
            });
        });
    }
    
    // Benchmark queries with complex expressions
    let complex_queries = [
        "SELECT id, name, age, (salary * 1.1) AS new_salary FROM employees WHERE department_id IN (1, 2, 3)",
        "SELECT product_id, SUM(quantity) FROM order_items GROUP BY product_id HAVING SUM(quantity) > 10",
        "SELECT u.id, u.name, COUNT(o.id) AS order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name",
    ];
    
    for (i, query) in complex_queries.iter().enumerate() {
        group.bench_with_input(BenchmarkId::new("complex_query", i), query, |b, query| {
            b.iter(|| {
                let mut parser = Parser::new(query);
                // These complex queries may not be fully implemented yet but we can still benchmark
                // what we have by using a match to handle results or errors
                match parser.parse_statement() {
                    Ok(stmt) => {
                        // Verify the statement is a valid SELECT statement
                        if let Statement::Select(_) = stmt {
                            // Valid select statement
                        }
                    },
                    Err(_) => {
                        // Some complex features might not be implemented yet
                    }
                }
            });
        });
    }
    
    group.finish();
}

criterion_group!(benches, query_parser_benchmark);
criterion_main!(benches); 