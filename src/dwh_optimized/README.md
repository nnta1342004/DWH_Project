# Data Warehouse Optimization Methods

This project demonstrates various optimization techniques for PostgreSQL data warehouses, focusing on query performance improvements through different approaches.

## Optimization Methods Implemented

### 1. Pre-aggregation with Materialized Views
- **Purpose**: Pre-compute and store aggregated results
- **Benefits**: Fast retrieval of common aggregations
- **Use Cases**: Dashboard metrics, regular reports

### 2. Secondary Index Optimization
- **Purpose**: Create indexes on frequently queried columns
- **Benefits**: Faster data retrieval through index lookups
- **Use Cases**: Filtered queries, sorted results

### 3. Probabilistic Data Structures (PDS)
- **Bloom Filter**: Fast existence checks with minimal false positives
- **HyperLogLog**: Approximate distinct count estimation
- **Count-Min Sketch**: Frequency estimation for high-cardinality data

### 4. Hybrid Optimization
- **Combination**: PDS + Materialized Views
- **Benefits**: Leverages strengths of both approaches
- **Use Cases**: Complex analytical queries with pre-filtering

## File Structure

```
src/dwh_optimized/
├── db_connection.py              # Database connection management
├── optimization_methods.py       # Core optimization implementations
├── setup_dwh.py                 # Data warehouse setup and initialization
├── benchmark_queries.py         # Performance benchmarking suite
├── pds_hybrid_optimization.py   # Hybrid PDS + MV optimization
└── README.md                    # This file
```

## Quick Start

1. **Install Dependencies**:
   ```powershell
   pip install -r requirements.txt
   ```

2. **Setup PostgreSQL Database**:
   - Ensure PostgreSQL is running on localhost:5432
   - Create database: `crm_erp`
   - Update connection parameters in `db_connection.py` if needed

3. **Run Complete Benchmark**:
   ```powershell
   .\run_optimization_benchmark.ps1
   ```

4. **Run Individual Tests**:
   ```powershell
   # Setup data warehouse
   python src/dwh_optimized/setup_dwh.py
   
   # Run basic benchmark
   python src/dwh_optimized/benchmark_queries.py
   
   # Run hybrid optimization test
   python src/dwh_optimized/pds_hybrid_optimization.py
   ```

## Performance Testing

The benchmark suite tests various query patterns:

1. **Daily Sales Aggregation**: Tests date-based grouping performance
2. **Customer Analysis**: Tests join performance with customer data
3. **Product Performance**: Tests product category analysis
4. **Top Customers**: Tests complex filtering and sorting
5. **Monthly Trends**: Tests time-series aggregation

Each test compares:
- Basic queries (no optimization)
- Materialized Views only
- Index optimization only
- PDS pre-filtering only
- Hybrid approach (PDS + Materialized Views)

## Expected Results

Based on typical performance characteristics:

- **Materialized Views**: 5-10x speedup for pre-aggregated queries
- **Indexes**: 2-5x speedup for filtered queries
- **PDS**: Near-instant approximate results with <5% error rate
- **Hybrid**: Combines benefits, often 3-8x speedup overall

## Configuration

### Database Connection
Update connection parameters in `db_connection.py`:
```python
DatabaseConnection(
    host="localhost",
    port=5432,
    database="crm_erp",
    user="postgres",
    password="your_password"
)
```

### PDS Parameters
Adjust PDS parameters in `optimization_methods.py`:
- Bloom Filter: `expected_items`, `false_positive_rate`
- HyperLogLog: `precision` (affects accuracy vs memory)
- Count-Min Sketch: `width`, `depth` (affects accuracy vs memory)

## Monitoring and Tuning

1. **Query Performance**: Monitor execution times in benchmark results
2. **PDS Accuracy**: Check error rates in accuracy tests
3. **Memory Usage**: Monitor PDS memory consumption
4. **Index Usage**: Use `EXPLAIN ANALYZE` to verify index usage

## Troubleshooting

### Common Issues

1. **Connection Errors**: Verify PostgreSQL is running and credentials are correct
2. **Import Errors**: Ensure all dependencies are installed
3. **Permission Errors**: Check database user permissions
4. **Memory Issues**: Reduce PDS parameters for large datasets

### Performance Issues

1. **Slow Queries**: Check if indexes are being used
2. **High Memory Usage**: Reduce PDS parameters
3. **Inaccurate Results**: Increase PDS precision parameters

## Advanced Usage

### Custom Queries
Add your own test queries to `benchmark_queries.py`:
```python
"custom_analysis": {
    "query": "SELECT ... FROM ... WHERE ...",
    "description": "Your custom analysis query"
}
```

### Custom PDS Structures
Create specialized PDS for your data:
```python
# Create custom Bloom filter
optimizer.pds.create_bloom_filter("custom_filter", expected_items=1000)

# Add data to filter
optimizer.pds.add_to_bloom_filter("custom_filter", "value")
```

### Custom Materialized Views
Add materialized views for your specific use cases:
```python
optimizer.create_materialized_view(
    "mv_custom_analysis",
    "SELECT ... FROM ... GROUP BY ..."
)
```

