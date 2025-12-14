import time
import pandas as pd
from typing import Dict, List, Any
from db_connection import DatabaseConnection
from optimization_methods import QueryOptimizer

class QueryBenchmark:
    def __init__(self, db_connection, optimizer):
        self.db = db_connection
        self.optimizer = optimizer
        self.results = []
    
    def run_benchmark_suite(self):
        print("=== Starting Query Performance Benchmark ===")
        
        # Define test queries
        test_queries = self.get_test_queries()
        
        # Run each query with different optimization methods
        for query_name, query_info in test_queries.items():
            print(f"\n--- Testing Query: {query_name} ---")
            self.run_query_variants(query_name, query_info)
        
        # Generate comparison report
        self.generate_report()
    
    def get_test_queries(self) -> Dict[str, Dict[str, Any]]:
        return {
            "daily_sales_aggregation": {
                "query": """
                    SELECT 
                        sale_date,
                        COUNT(*) as total_sales,
                        SUM(net_amount) as total_revenue,
                        AVG(net_amount) as avg_sale_amount
                    FROM fact_sales 
                    WHERE sale_date >= '2024-01-01'
                    GROUP BY sale_date
                    ORDER BY sale_date
                """,
                "description": "Daily sales aggregation with date filter"
            },
            
            "customer_analysis": {
                "query": """
                    SELECT 
                        c.region,
                        COUNT(DISTINCT c.customer_id) as unique_customers,
                        COUNT(f.sale_id) as total_sales,
                        SUM(f.net_amount) as total_revenue,
                        AVG(f.net_amount) as avg_purchase
                    FROM dim_customers c
                    LEFT JOIN fact_sales f ON c.customer_id = f.customer_id
                    WHERE f.sale_date >= '2024-01-01'
                    GROUP BY c.region
                    ORDER BY total_revenue DESC
                """,
                "description": "Customer analysis by region"
            },
            
            "product_performance": {
                "query": """
                    SELECT 
                        p.category_name,
                        COUNT(f.sale_id) as total_sales,
                        SUM(f.quantity) as total_quantity,
                        SUM(f.net_amount) as total_revenue,
                        AVG(f.unit_price) as avg_price
                    FROM dim_products p
                    LEFT JOIN fact_sales f ON p.product_id = f.product_id
                    WHERE f.sale_date >= '2024-01-01'
                    GROUP BY p.category_name
                    ORDER BY total_revenue DESC
                """,
                "description": "Product performance by category"
            },
            
            "top_customers": {
                "query": """
                    SELECT 
                        c.customer_name,
                        c.region,
                        COUNT(f.sale_id) as purchase_count,
                        SUM(f.net_amount) as total_spent
                    FROM dim_customers c
                    INNER JOIN fact_sales f ON c.customer_id = f.customer_id
                    WHERE f.sale_date >= '2024-01-01'
                    GROUP BY c.customer_id, c.customer_name, c.region
                    HAVING COUNT(f.sale_id) > 5
                    ORDER BY total_spent DESC
                    LIMIT 10
                """,
                "description": "Top customers by spending"
            },
            
            "monthly_trends": {
                "query": """
                    SELECT 
                        EXTRACT(YEAR FROM sale_date) as year,
                        EXTRACT(MONTH FROM sale_date) as month,
                        COUNT(*) as sales_count,
                        SUM(net_amount) as monthly_revenue,
                        AVG(net_amount) as avg_sale_amount
                    FROM fact_sales
                    WHERE sale_date >= '2024-01-01'
                    GROUP BY EXTRACT(YEAR FROM sale_date), EXTRACT(MONTH FROM sale_date)
                    ORDER BY year, month
                """,
                "description": "Monthly sales trends"
            }
        }
    
    def run_query_variants(self, query_name: str, query_info: Dict[str, Any]):
        base_query = query_info["query"]
        description = query_info["description"]
        
        # Test 1: Basic query (no optimization)
        result = self.benchmark_query(
            f"{query_name}_basic",
            base_query,
            "Basic Query (No Optimization)",
            description
        )
        
        # Test 2: Using materialized view (if applicable)
        if "daily_sales" in query_name:
            mv_query = "SELECT * FROM mv_daily_sales WHERE sale_date >= '2024-01-01'"
            self.benchmark_query(
                f"{query_name}_materialized_view",
                mv_query,
                "Materialized View",
                f"{description} - Using Materialized View"
            )
        
        # Test 3: Using indexes (already created)
        result = self.benchmark_query(
            f"{query_name}_with_indexes",
            base_query,
            "With Indexes",
            f"{description} - With Indexes"
        )
        
        # Test 4: Using PDS for pre-filtering
        if "customer" in query_name:
            self.benchmark_with_pds_filter(query_name, base_query, description)
    
    def benchmark_with_pds_filter(self, query_name: str, base_query: str, description: str):
        """Test query with PDS pre-filtering"""
        print(f"  Testing PDS pre-filtering...")
        
        # Simulate PDS pre-filtering by adding WHERE conditions based on Bloom filters
        pds_query = base_query.replace(
            "WHERE f.sale_date >= '2024-01-01'",
            """WHERE f.sale_date >= '2024-01-01' 
               AND EXISTS (SELECT 1 FROM dim_customers c WHERE c.customer_id = f.customer_id)"""
        )
        
        self.benchmark_query(
            f"{query_name}_pds_filter",
            pds_query,
            "PDS Pre-filtering",
            f"{description} - With PDS Pre-filtering"
        )
    
    def benchmark_query(self, test_name: str, query: str, method: str, description: str, 
                       iterations: int = 5) -> Dict[str, Any]:
        print(f"  Testing {method}...")
        
        times = []
        result_rows = 0
        
        for i in range(iterations):
            start_time = time.time()
            try:
                result = self.db.execute_query(query)
                end_time = time.time()
                times.append(end_time - start_time)
                result_rows = len(result)
            except Exception as e:
                print(f"    Error in iteration {i+1}: {e}")
                times.append(float('inf'))
        
        # Calculate statistics
        valid_times = [t for t in times if t != float('inf')]
        if not valid_times:
            avg_time = float('inf')
            min_time = float('inf')
            max_time = float('inf')
        else:
            avg_time = sum(valid_times) / len(valid_times)
            min_time = min(valid_times)
            max_time = max(valid_times)
        
        result = {
            'test_name': test_name,
            'method': method,
            'description': description,
            'avg_time': avg_time,
            'min_time': min_time,
            'max_time': max_time,
            'iterations': iterations,
            'result_rows': result_rows,
            'success_rate': len(valid_times) / iterations * 100
        }
        
        self.results.append(result)
        
        print(f"    Avg: {avg_time:.4f}s, Min: {min_time:.4f}s, Max: {max_time:.4f}s, Rows: {result_rows}")
        
        return result
    
    def generate_report(self):
        print("\n" + "="*80)
        print("QUERY PERFORMANCE BENCHMARK REPORT")
        print("="*80)
        
        if not self.results:
            print("No benchmark results available.")
            return
        
        # Group results by test name
        grouped_results = {}
        for result in self.results:
            base_name = result['test_name'].split('_')[0] + '_' + result['test_name'].split('_')[1]
            if base_name not in grouped_results:
                grouped_results[base_name] = []
            grouped_results[base_name].append(result)
        
        # Generate comparison for each query
        for base_name, results in grouped_results.items():
            print(f"\n--- {base_name.upper().replace('_', ' ')} ---")
            print(f"{'Method':<25} {'Avg Time (s)':<12} {'Min Time (s)':<12} {'Max Time (s)':<12} {'Rows':<8} {'Success %':<10}")
            print("-" * 80)
            
            # Sort by average time
            sorted_results = sorted(results, key=lambda x: x['avg_time'])
            
            for result in sorted_results:
                print(f"{result['method']:<25} {result['avg_time']:<12.4f} {result['min_time']:<12.4f} "
                      f"{result['max_time']:<12.4f} {result['result_rows']:<8} {result['success_rate']:<10.1f}")
            
            # Calculate speedup
            if len(sorted_results) > 1:
                baseline_time = sorted_results[0]['avg_time']
                print(f"\nSpeedup compared to fastest method:")
                for result in sorted_results[1:]:
                    if result['avg_time'] != float('inf') and baseline_time != 0:
                        speedup = result['avg_time'] / baseline_time
                        print(f"  {result['method']}: {speedup:.2f}x slower")
        
        # Overall summary
        print(f"\n--- OVERALL SUMMARY ---")
        print(f"Total tests run: {len(self.results)}")
        
        # Find best performing method overall
        valid_results = [r for r in self.results if r['avg_time'] != float('inf')]
        if valid_results:
            best_result = min(valid_results, key=lambda x: x['avg_time'])
            print(f"Best performing method: {best_result['method']} ({best_result['avg_time']:.4f}s)")
            
            # Count wins by method
            method_wins = {}
            for base_name, results in grouped_results.items():
                valid_results_group = [r for r in results if r['avg_time'] != float('inf')]
                if valid_results_group:
                    best_in_group = min(valid_results_group, key=lambda x: x['avg_time'])
                    method = best_in_group['method']
                    method_wins[method] = method_wins.get(method, 0) + 1
            
            print(f"\nMethod performance ranking:")
            for method, wins in sorted(method_wins.items(), key=lambda x: x[1], reverse=True):
                print(f"  {method}: {wins} wins")
    
    def test_pds_accuracy(self):
        print("\n--- Testing PDS Accuracy ---")
        
        # Test HyperLogLog accuracy
        print("HyperLogLog Accuracy Test:")
        hll_customer_count = self.optimizer.pds.get_hyperloglog_count("fact_sales_customer_id_hll")
        actual_customer_count = len(self.db.execute_query("SELECT DISTINCT customer_id FROM fact_sales"))
        error_rate = abs(hll_customer_count - actual_customer_count) / actual_customer_count * 100
        
        print(f"  Estimated distinct customers: {hll_customer_count}")
        print(f"  Actual distinct customers: {actual_customer_count}")
        print(f"  Error rate: {error_rate:.2f}%")
        
        # Test Count-Min Sketch accuracy
        print("\nCount-Min Sketch Accuracy Test:")
        test_customers = ['CUST001', 'CUST002', 'CUST003']
        for customer in test_customers:
            estimated_count = self.optimizer.pds.get_count_min_sketch_count("fact_sales_customer_id_cm", customer)
            actual_count = len(self.db.execute_query("SELECT * FROM fact_sales WHERE customer_id = %s", (customer,)))
            error_rate = abs(estimated_count - actual_count) / max(actual_count, 1) * 100
            
            print(f"  Customer {customer}:")
            print(f"    Estimated count: {estimated_count}")
            print(f"    Actual count: {actual_count}")
            print(f"    Error rate: {error_rate:.2f}%")

if __name__ == "__main__":
    from setup_dwh import setup_data_warehouse
    
    # Setup data warehouse
    db, optimizer = setup_data_warehouse()
    
    # Run benchmark
    benchmark = QueryBenchmark(db, optimizer)
    benchmark.run_benchmark_suite()
    benchmark.test_pds_accuracy()

