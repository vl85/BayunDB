#!/bin/bash
# Script to run SQLite comparison benchmarks and generate reports

set -e

echo "===== Running SQLite Comparison Benchmarks ====="

# Create output directories
mkdir -p benches/sqlite_comparison/reports
mkdir -p benches/sqlite_comparison/reports/charts

# Run the benchmarks
echo "Running benchmarks..."
cargo bench --bench sqlite_comparison_bench

# Generate reports
echo "Generating reports..."
# This would normally be done by the benchmark itself in the generate_report function
# But here we could run any additional post-processing scripts

# Convert JSON to pretty Markdown tables or charts
# Example (placeholder):
# python3 scripts/generate_benchmark_charts.py

echo "===== Benchmark Run Complete ====="
echo "Reports saved to benches/sqlite_comparison/reports/"

# Optional: Open the reports
if command -v xdg-open > /dev/null; then
    xdg-open benches/sqlite_comparison/reports/
elif command -v open > /dev/null; then
    open benches/sqlite_comparison/reports/
fi 