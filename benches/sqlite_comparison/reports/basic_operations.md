# Basic Operations Benchmark Report

Generated: 2025-05-13T23:05:30.199885600+00:00

## System Information

- OS: windows
- CPU: AMD64 Family 23 Model 113 Stepping 0, AuthenticAMD
- Memory: Unknown
- Rust: 1.75.0
- SQLite: 3.41.2
- BayunDB: 0.1.0

## Simple SELECT Performance

| Database | Rows Affected | Time (ms) | Ratio to SQLite |
|----------|---------------|-----------|----------------|
| SQLite | 100 | 1.80 | 1.00x |
| BayunDB | 100 | 2.30 | 1.28x |

## Table Creation (100 rows) Performance

| Database | Rows Affected | Time (ms) | Ratio to SQLite |
|----------|---------------|-----------|----------------|
| SQLite | 100 | 2.50 | 1.00x |
| BayunDB | 100 | 3.20 | 1.28x |

