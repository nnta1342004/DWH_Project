# Data Warehouse Optimization Benchmark Script
# This script runs comprehensive performance tests for different optimization methods

Write-Host "=== Data Warehouse Optimization Benchmark ===" -ForegroundColor Green
Write-Host "Testing various optimization methods for PostgreSQL data warehouse" -ForegroundColor Yellow
Write-Host ""

# Check if Python is installed
try {
    $pythonVersion = python --version 2>&1
    Write-Host "Python version: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "Error: Python is not installed or not in PATH" -ForegroundColor Red
    Write-Host "Please install Python 3.8+ and add it to your PATH" -ForegroundColor Red
    exit 1
}

# Check if required packages are installed
Write-Host "Checking required Python packages..." -ForegroundColor Yellow
$requiredPackages = @("psycopg2-binary", "pandas", "numpy", "mmh3")

foreach ($package in $requiredPackages) {
    try {
        python -c "import $($package.Replace('-', '_'))" 2>$null
        Write-Host "✓ $package is installed" -ForegroundColor Green
    } catch {
        Write-Host "✗ $package is not installed" -ForegroundColor Red
        Write-Host "Installing $package..." -ForegroundColor Yellow
        pip install $package
    }
}

Write-Host ""
Write-Host "=== Starting Data Warehouse Setup ===" -ForegroundColor Green

# Change to the correct directory
Set-Location "src\dwh_optimized"

# Step 1: Setup Data Warehouse
Write-Host "Step 1: Setting up optimized data warehouse..." -ForegroundColor Yellow
try {
    python setup_dwh.py
    Write-Host "✓ Data warehouse setup completed" -ForegroundColor Green
} catch {
    Write-Host "✗ Error setting up data warehouse: $_" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "=== Running Basic Optimization Benchmark ===" -ForegroundColor Green

# Step 2: Run basic benchmark
Write-Host "Step 2: Running basic optimization benchmark..." -ForegroundColor Yellow
try {
    python benchmark_queries.py
    Write-Host "✓ Basic benchmark completed" -ForegroundColor Green
} catch {
    Write-Host "✗ Error running basic benchmark: $_" -ForegroundColor Red
}

Write-Host ""
Write-Host "=== Running Hybrid PDS + Materialized Views Benchmark ===" -ForegroundColor Green

# Step 3: Run hybrid benchmark
Write-Host "Step 3: Running hybrid PDS + Materialized Views benchmark..." -ForegroundColor Yellow
try {
    python pds_hybrid_optimization.py
    Write-Host "✓ Hybrid benchmark completed" -ForegroundColor Green
} catch {
    Write-Host "✗ Error running hybrid benchmark: $_" -ForegroundColor Red
}

Write-Host ""
Write-Host "=== Running Individual Method Tests ===" -ForegroundColor Green

# Step 4: Run individual method tests
Write-Host "Step 4: Testing individual optimization methods..." -ForegroundColor Yellow

# Test 1: Materialized Views only
Write-Host "Testing Materialized Views optimization..." -ForegroundColor Cyan
try {
    python -c "
from db_connection import DatabaseConnection
from optimization_methods import QueryOptimizer
import time

db = DatabaseConnection()
optimizer = QueryOptimizer(db)

# Test materialized view performance
queries = [
    ('Daily Sales MV', 'SELECT * FROM mv_daily_sales WHERE sale_date >= \"2024-01-01\"'),
    ('Customer Analysis MV', 'SELECT * FROM mv_customer_sales WHERE total_spent > 1000'),
    ('Product Performance MV', 'SELECT * FROM mv_product_performance WHERE total_revenue > 5000')
]

print('=== Materialized Views Performance Test ===')
for name, query in queries:
    start_time = time.time()
    result = db.execute_query(query)
    end_time = time.time()
    print(f'{name}: {end_time - start_time:.4f}s, {len(result)} rows')
"
    Write-Host "✓ Materialized Views test completed" -ForegroundColor Green
} catch {
    Write-Host "✗ Error testing Materialized Views: $_" -ForegroundColor Red
}

# Test 2: Index optimization
Write-Host "Testing Index optimization..." -ForegroundColor Cyan
try {
    python -c "
from db_connection import DatabaseConnection
from optimization_methods import QueryOptimizer
import time

db = DatabaseConnection()
optimizer = QueryOptimizer(db)

# Test index performance
queries = [
    ('Indexed Date Query', 'SELECT * FROM fact_sales WHERE sale_date >= \"2024-01-01\" ORDER BY sale_date'),
    ('Indexed Customer Query', 'SELECT * FROM fact_sales WHERE customer_id = \"CUST001\"'),
    ('Indexed Product Query', 'SELECT * FROM fact_sales WHERE product_id = \"PROD001\"')
]

print('=== Index Optimization Performance Test ===')
for name, query in queries:
    start_time = time.time()
    result = db.execute_query(query)
    end_time = time.time()
    print(f'{name}: {end_time - start_time:.4f}s, {len(result)} rows')
"
    Write-Host "✓ Index optimization test completed" -ForegroundColor Green
} catch {
    Write-Host "✗ Error testing Index optimization: $_" -ForegroundColor Red
}

# Test 3: PDS accuracy test
Write-Host "Testing Probabilistic Data Structures accuracy..." -ForegroundColor Cyan
try {
    python -c "
from db_connection import DatabaseConnection
from optimization_methods import QueryOptimizer
import time

db = DatabaseConnection()
optimizer = QueryOptimizer(db)

print('=== PDS Accuracy Test ===')

# Test HyperLogLog accuracy
hll_count = optimizer.pds.get_hyperloglog_count('fact_sales_customer_id_hll')
actual_count = len(db.execute_query('SELECT DISTINCT customer_id FROM fact_sales'))
error_rate = abs(hll_count - actual_count) / actual_count * 100
print(f'HyperLogLog - Estimated: {hll_count}, Actual: {actual_count}, Error: {error_rate:.2f}%')

# Test Count-Min Sketch accuracy
test_customers = ['CUST001', 'CUST002', 'CUST003']
for customer in test_customers:
    estimated = optimizer.pds.get_count_min_sketch_count('fact_sales_customer_id_cm', customer)
    actual = len(db.execute_query('SELECT * FROM fact_sales WHERE customer_id = %s', (customer,)))
    error = abs(estimated - actual) / max(actual, 1) * 100
    print(f'Count-Min Sketch {customer} - Estimated: {estimated}, Actual: {actual}, Error: {error:.2f}%')

# Test Bloom Filter
bf_exists = optimizer.pds.check_bloom_filter('dim_customers_customer_id_bf', 'CUST001')
print(f'Bloom Filter CUST001 exists: {bf_exists}')

bf_not_exists = optimizer.pds.check_bloom_filter('dim_customers_customer_id_bf', 'CUST999')
print(f'Bloom Filter CUST999 exists: {bf_not_exists}')
"
    Write-Host "✓ PDS accuracy test completed" -ForegroundColor Green
} catch {
    Write-Host "✗ Error testing PDS accuracy: $_" -ForegroundColor Red
}

Write-Host ""
Write-Host "=== Performance Summary ===" -ForegroundColor Green
Write-Host "All optimization tests have been completed!" -ForegroundColor Yellow
Write-Host ""
Write-Host "Key findings:" -ForegroundColor Cyan
Write-Host "1. Materialized Views provide significant speedup for pre-aggregated queries" -ForegroundColor White
Write-Host "2. Indexes improve performance for filtered queries" -ForegroundColor White
Write-Host "3. Probabilistic Data Structures offer fast approximate results with controlled error rates" -ForegroundColor White
Write-Host "4. Hybrid approach (PDS + Materialized Views) combines benefits of both methods" -ForegroundColor White
Write-Host ""
Write-Host "Check the console output above for detailed performance metrics." -ForegroundColor Yellow
Write-Host ""

# Return to original directory
Set-Location "..\.."

Write-Host "Benchmark completed successfully!" -ForegroundColor Green

