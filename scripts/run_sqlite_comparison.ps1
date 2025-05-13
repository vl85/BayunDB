# PowerShell script to run SQLite comparison benchmarks and generate reports

Write-Host "===== Running SQLite Comparison Benchmarks =====" -ForegroundColor Green

# Create output directories
New-Item -ItemType Directory -Force -Path "benches\sqlite_comparison\reports" | Out-Null
New-Item -ItemType Directory -Force -Path "benches\sqlite_comparison\reports\charts" | Out-Null

# Check and install Python dependencies
Write-Host "Checking Python dependencies..." -ForegroundColor Cyan
$pythonCmd = $null
if (Get-Command python -ErrorAction SilentlyContinue) {
    $pythonCmd = "python"
} elseif (Get-Command python3 -ErrorAction SilentlyContinue) {
    $pythonCmd = "python3"
} else {
    Write-Host "Python not found. Please install Python to generate charts." -ForegroundColor Yellow
}

if ($pythonCmd) {
    Write-Host "Installing required Python packages..." -ForegroundColor Cyan
    & $pythonCmd -m pip install -r scripts/requirements.txt
}

# Run the benchmarks
Write-Host "Running benchmarks..." -ForegroundColor Cyan
cargo bench --bench sqlite_comparison_bench

# Generate reports
Write-Host "Generating reports..." -ForegroundColor Cyan
# Run the Python script to generate charts from benchmark results
if ($pythonCmd -and (Test-Path "benches\sqlite_comparison\reports\*.json")) {
    & $pythonCmd scripts/generate_benchmark_charts.py
}

Write-Host "===== Benchmark Run Complete =====" -ForegroundColor Green
Write-Host "Reports saved to benches\sqlite_comparison\reports\" -ForegroundColor Green

# Open the reports folder
try {
    Invoke-Item "benches\sqlite_comparison\reports\"
} catch {
    Write-Host "Could not open reports folder. Navigate to benches\sqlite_comparison\reports\ to view results." -ForegroundColor Yellow
} 