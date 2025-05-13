#!/usr/bin/env python3
"""
Script to generate charts from benchmark JSON data.
This script reads the JSON output from the SQLite comparison benchmarks
and generates charts for visualization.

Dependencies:
- matplotlib
- pandas

Usage:
    python3 generate_benchmark_charts.py
"""

import json
import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

def load_benchmark_data(json_file):
    """Load benchmark results from a JSON file."""
    with open(json_file, 'r') as f:
        return json.load(f)

def group_results_by_operation(results):
    """Group benchmark results by operation."""
    grouped = {}
    for result in results:
        op = result['operation']
        if op not in grouped:
            grouped[op] = []
        grouped[op].append(result)
    return grouped

def create_bar_chart(operation, results, output_dir):
    """Create a bar chart comparing SQLite and BayunDB for a specific operation."""
    # Extract data
    databases = [r['database'] for r in results]
    times = [r['execution_time_ms'] for r in results]
    
    # Create DataFrame
    df = pd.DataFrame({
        'Database': databases,
        'Time (ms)': times
    })
    
    # Create the bar chart
    plt.figure(figsize=(10, 6))
    bars = plt.bar(df['Database'], df['Time (ms)'], color=['#1f77b4', '#ff7f0e'])
    
    # Add values on top of bars
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                 f'{height:.2f}', ha='center', va='bottom')
    
    # Add ratio text
    if len(times) == 2 and 'SQLite' in databases:
        sqlite_idx = databases.index('SQLite')
        sqlite_time = times[sqlite_idx]
        other_idx = 1 - sqlite_idx
        other_time = times[other_idx]
        ratio = other_time / sqlite_time
        plt.text(0.5, 0.95, f'BayunDB/SQLite Ratio: {ratio:.2f}x', 
                 ha='center', va='center', transform=plt.gca().transAxes,
                 bbox=dict(facecolor='white', alpha=0.5))
    
    # Add labels and title
    plt.ylabel('Execution Time (ms)')
    plt.title(f'{operation} Performance Comparison')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    
    # Save the chart
    safe_op_name = operation.replace(' ', '_').replace('(', '').replace(')', '').lower()
    output_file = os.path.join(output_dir, f'{safe_op_name}_chart.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Generated chart: {output_file}")
    return output_file

def main():
    """Main function to process benchmark results and generate charts."""
    # Directories
    bench_dir = Path("benches/sqlite_comparison/reports")
    charts_dir = bench_dir / "charts"
    
    # Create charts directory if it doesn't exist
    os.makedirs(charts_dir, exist_ok=True)
    
    # Find all JSON files in the reports directory
    json_files = list(bench_dir.glob("*.json"))
    
    if not json_files:
        print("No benchmark JSON files found in", bench_dir)
        return 1
    
    # Process each JSON file
    for json_file in json_files:
        print(f"Processing {json_file}...")
        try:
            # Load benchmark data
            report = load_benchmark_data(json_file)
            
            # Group results by operation
            grouped_results = group_results_by_operation(report['results'])
            
            # Generate a chart for each operation
            chart_files = []
            for operation, results in grouped_results.items():
                chart_file = create_bar_chart(operation, results, charts_dir)
                chart_files.append(chart_file)
            
            # Generate summary markdown with embedded charts
            summary_md = bench_dir / f"{json_file.stem}_summary.md"
            with open(summary_md, 'w') as f:
                f.write(f"# {report['name']} Benchmark Summary\n\n")
                f.write(f"Generated: {report['timestamp']}\n\n")
                
                f.write("## System Information\n\n")
                for key, value in report['system_info'].items():
                    f.write(f"- **{key.capitalize()}**: {value}\n")
                f.write("\n")
                
                f.write("## Performance Charts\n\n")
                for chart_file in chart_files:
                    rel_path = os.path.relpath(chart_file, bench_dir.parent.parent)
                    f.write(f"![{os.path.basename(chart_file)}]({rel_path})\n\n")
            
            print(f"Generated summary: {summary_md}")
            
        except Exception as e:
            print(f"Error processing {json_file}: {e}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 