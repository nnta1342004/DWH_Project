import time
import pandas as pd
from typing import Dict, List, Any, Tuple
from db_connection import DatabaseConnection
from optimization_methods import QueryOptimizer, ProbabilisticDataStructures

class PDSHybridOptimizer:
    def __init__(self, db_connection, optimizer):
        self.db = db_connection
        self.optimizer = optimizer
        self.pds = optimizer.pds
        self.hybrid_results = {}
    
    def setup_hybrid_optimization(self):
        """Setup combined PDS + Materialized Views optimization"""
        print("=== Setting up PDS + Materialized Views Hybrid Optimization ===")
        
        # Create enhanced materialized views with PDS metadata
        self.create_pds_enhanced_views()
        
        # Setup PDS for common filter columns
        self.setup_hybrid_pds()
        
        print("=== Hybrid Optimization Setup Complete ===")
    
    def create_pds_enhanced_views(self):
        """Create materialized views with PDS metadata columns"""
        
        # Enhanced daily sales view with PDS metadata
        enhanced_daily_sales = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_sales_enhanced AS
        SELECT 
            sale_date,
            COUNT(*) as total_sales,
            SUM(net_amount) as total_revenue,
            AVG(net_amount) as avg_sale_amount,
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(DISTINCT product_id) as unique_products,
            -- PDS metadata for quick filtering
            MIN(customer_id) as min_customer_id,
            MAX(customer_id) as max_customer_id,
            MIN(product_id) as min_product_id,
            MAX(product_id) as max_product_id
        FROM fact_sales 
        GROUP BY sale_date
        ORDER BY sale_date
        """
        
        # Enhanced customer analysis view
        enhanced_customer_analysis = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_customer_analysis_enhanced AS
        SELECT 
            c.customer_id,
            c.customer_name,
            c.region,
            c.gender,
            COUNT(f.sale_id) as total_purchases,
            SUM(f.net_amount) as total_spent,
            AVG(f.net_amount) as avg_purchase_amount,
            MIN(f.sale_date) as first_purchase_date,
            MAX(f.sale_date) as last_purchase_date,
            COUNT(DISTINCT f.product_id) as unique_products_bought,
            -- PDS metadata
            MIN(f.product_id) as min_product_id,
            MAX(f.product_id) as max_product_id
        FROM dim_customers c
        LEFT JOIN fact_sales f ON c.customer_id = f.customer_id
        GROUP BY c.customer_id, c.customer_name, c.region, c.gender
        """
        
        # Enhanced product performance view
        enhanced_product_performance = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_product_performance_enhanced AS
        SELECT 
            p.product_id,
            p.product_name,
            p.category_name,
            p.unit_price,
            COUNT(f.sale_id) as total_sales,
            SUM(f.quantity) as total_quantity_sold,
            SUM(f.net_amount) as total_revenue,
            AVG(f.unit_price) as avg_sale_price,
            COUNT(DISTINCT f.customer_id) as unique_customers,
            -- PDS metadata
            MIN(f.customer_id) as min_customer_id,
            MAX(f.customer_id) as max_customer_id
        FROM dim_products p
        LEFT JOIN fact_sales f ON p.product_id = f.product_id
        GROUP BY p.product_id, p.product_name, p.category_name, p.unit_price
        """
        
        views = [
            ("mv_daily_sales_enhanced", enhanced_daily_sales),
            ("mv_customer_analysis_enhanced", enhanced_customer_analysis),
            ("mv_product_performance_enhanced", enhanced_product_performance)
        ]
        
        for view_name, view_query in views:
            self.db.execute_command(view_query)
            print(f"Created enhanced materialized view: {view_name}")
    
    def setup_hybrid_pds(self):
        """Setup PDS structures for hybrid optimization"""
        
        # Create Bloom filters for common filter values
        self.create_filter_bloom_filters()
        
        # Create HyperLogLog for distinct count estimation
        self.create_estimation_hyperloglog()
        
        # Create Count-Min Sketch for frequency estimation
        self.create_frequency_count_min_sketches()
        
        print("PDS structures created for hybrid optimization")
    
    def create_filter_bloom_filters(self):
        """Create Bloom filters for common filter values"""
        
        # Customer region filter
        regions = self.db.execute_query("SELECT DISTINCT region FROM dim_customers")
        bf_name = "customer_regions_bf"
        self.pds.create_bloom_filter(bf_name, len(regions) * 2)
        
        for region in regions['region'].dropna():
            self.pds.add_to_bloom_filter(bf_name, str(region))
        
        # Product category filter
        categories = self.db.execute_query("SELECT DISTINCT category_name FROM dim_products")
        bf_name = "product_categories_bf"
        self.pds.create_bloom_filter(bf_name, len(categories) * 2)
        
        for category in categories['category_name'].dropna():
            self.pds.add_to_bloom_filter(bf_name, str(category))
        
        # Date range filter (for common date patterns)
        dates = self.db.execute_query("SELECT DISTINCT sale_date FROM fact_sales ORDER BY sale_date")
        bf_name = "sale_dates_bf"
        self.pds.create_bloom_filter(bf_name, len(dates) * 2)
        
        for date in dates['sale_date'].dropna():
            self.pds.add_to_bloom_filter(bf_name, str(date))
        
        print("Created filter Bloom filters")
    
    def create_estimation_hyperloglog(self):
        """Create HyperLogLog for distinct count estimation"""
        
        # Customer-product combinations
        combinations = self.db.execute_query("""
            SELECT DISTINCT customer_id, product_id 
            FROM fact_sales
        """)
        
        hll_name = "customer_product_combinations_hll"
        self.pds.create_hyperloglog(hll_name)
        
        for _, row in combinations.iterrows():
            combination = f"{row['customer_id']}_{row['product_id']}"
            self.pds.add_to_hyperloglog(hll_name, combination)
        
        print("Created estimation HyperLogLog structures")
    
    def create_frequency_count_min_sketches(self):
        """Create Count-Min Sketch for frequency estimation"""
        
        # Customer purchase frequency
        cm_name = "customer_purchase_frequency_cm"
        self.pds.create_count_min_sketch(cm_name)
        
        customer_purchases = self.db.execute_query("""
            SELECT customer_id, COUNT(*) as purchase_count
            FROM fact_sales
            GROUP BY customer_id
        """)
        
        for _, row in customer_purchases.iterrows():
            self.pds.add_to_count_min_sketch(
                cm_name, 
                str(row['customer_id']), 
                int(row['purchase_count'])
            )
        
        # Product sales frequency
        cm_name = "product_sales_frequency_cm"
        self.pds.create_count_min_sketch(cm_name)
        
        product_sales = self.db.execute_query("""
            SELECT product_id, COUNT(*) as sales_count
            FROM fact_sales
            GROUP BY product_id
        """)
        
        for _, row in product_sales.iterrows():
            self.pds.add_to_count_min_sketch(
                cm_name, 
                str(row['product_id']), 
                int(row['sales_count'])
            )
        
        print("Created frequency Count-Min Sketch structures")
    
    def run_hybrid_benchmark(self):
        """Run benchmark comparing different optimization approaches"""
        print("\n=== Running Hybrid Optimization Benchmark ===")
        
        test_queries = self.get_hybrid_test_queries()
        results = []
        
        for test_name, query_info in test_queries.items():
            print(f"\n--- Testing: {test_name} ---")
            
            # Test 1: Basic query
            result1 = self.benchmark_hybrid_query(
                f"{test_name}_basic",
                query_info["basic_query"],
                "Basic Query",
                query_info["description"]
            )
            
            # Test 2: Materialized view only
            result2 = self.benchmark_hybrid_query(
                f"{test_name}_mv_only",
                query_info["mv_query"],
                "Materialized View Only",
                query_info["description"]
            )
            
            # Test 3: PDS pre-filtering only
            result3 = self.benchmark_hybrid_query(
                f"{test_name}_pds_only",
                query_info["pds_query"],
                "PDS Pre-filtering Only",
                query_info["description"]
            )
            
            # Test 4: Hybrid approach (PDS + MV)
            result4 = self.benchmark_hybrid_query(
                f"{test_name}_hybrid",
                query_info["hybrid_query"],
                "Hybrid (PDS + MV)",
                query_info["description"]
            )
            
            results.extend([result1, result2, result3, result4])
        
        self.generate_hybrid_report(results)
        return results
    
    def get_hybrid_test_queries(self) -> Dict[str, Dict[str, Any]]:
        return {
            "daily_sales_analysis": {
                "description": "Daily sales analysis with region filter",
                "basic_query": """
                    SELECT 
                        f.sale_date,
                        c.region,
                        COUNT(*) as total_sales,
                        SUM(f.net_amount) as total_revenue,
                        AVG(f.net_amount) as avg_sale_amount
                    FROM fact_sales f
                    JOIN dim_customers c ON f.customer_id = c.customer_id
                    WHERE f.sale_date >= '2024-01-01'
                    AND c.region IN ('NY', 'CA', 'IL')
                    GROUP BY f.sale_date, c.region
                    ORDER BY f.sale_date, c.region
                """,
                "mv_query": """
                    SELECT 
                        f.sale_date,
                        c.region,
                        COUNT(*) as total_sales,
                        SUM(f.net_amount) as total_revenue,
                        AVG(f.net_amount) as avg_sale_amount
                    FROM mv_daily_sales_enhanced f
                    JOIN dim_customers c ON f.min_customer_id <= c.customer_id AND f.max_customer_id >= c.customer_id
                    WHERE f.sale_date >= '2024-01-01'
                    AND c.region IN ('NY', 'CA', 'IL')
                    GROUP BY f.sale_date, c.region
                    ORDER BY f.sale_date, c.region
                """,
                "pds_query": """
                    SELECT 
                        f.sale_date,
                        c.region,
                        COUNT(*) as total_sales,
                        SUM(f.net_amount) as total_revenue,
                        AVG(f.net_amount) as avg_sale_amount
                    FROM fact_sales f
                    JOIN dim_customers c ON f.customer_id = c.customer_id
                    WHERE f.sale_date >= '2024-01-01'
                    AND c.region IN ('NY', 'CA', 'IL')
                    -- PDS pre-filtering simulation
                    AND EXISTS (SELECT 1 FROM dim_customers c2 WHERE c2.customer_id = f.customer_id)
                    GROUP BY f.sale_date, c.region
                    ORDER BY f.sale_date, c.region
                """,
                "hybrid_query": """
                    SELECT 
                        f.sale_date,
                        c.region,
                        f.total_sales,
                        f.total_revenue,
                        f.avg_sale_amount
                    FROM mv_daily_sales_enhanced f
                    JOIN dim_customers c ON f.min_customer_id <= c.customer_id AND f.max_customer_id >= c.customer_id
                    WHERE f.sale_date >= '2024-01-01'
                    AND c.region IN ('NY', 'CA', 'IL')
                    -- PDS pre-filtering
                    AND EXISTS (SELECT 1 FROM dim_customers c2 WHERE c2.customer_id = f.min_customer_id)
                    ORDER BY f.sale_date, c.region
                """
            },
            
            "customer_segmentation": {
                "description": "Customer segmentation by spending patterns",
                "basic_query": """
                    SELECT 
                        c.region,
                        c.gender,
                        COUNT(DISTINCT c.customer_id) as unique_customers,
                        SUM(f.net_amount) as total_revenue,
                        AVG(f.net_amount) as avg_purchase_amount
                    FROM dim_customers c
                    LEFT JOIN fact_sales f ON c.customer_id = f.customer_id
                    WHERE f.sale_date >= '2024-01-01'
                    GROUP BY c.region, c.gender
                    HAVING COUNT(f.sale_id) > 0
                    ORDER BY total_revenue DESC
                """,
                "mv_query": """
                    SELECT 
                        c.region,
                        c.gender,
                        COUNT(DISTINCT c.customer_id) as unique_customers,
                        SUM(ca.total_spent) as total_revenue,
                        AVG(ca.avg_purchase_amount) as avg_purchase_amount
                    FROM dim_customers c
                    LEFT JOIN mv_customer_analysis_enhanced ca ON c.customer_id = ca.customer_id
                    WHERE ca.first_purchase_date >= '2024-01-01'
                    GROUP BY c.region, c.gender
                    HAVING COUNT(ca.customer_id) > 0
                    ORDER BY total_revenue DESC
                """,
                "pds_query": """
                    SELECT 
                        c.region,
                        c.gender,
                        COUNT(DISTINCT c.customer_id) as unique_customers,
                        SUM(f.net_amount) as total_revenue,
                        AVG(f.net_amount) as avg_purchase_amount
                    FROM dim_customers c
                    LEFT JOIN fact_sales f ON c.customer_id = f.customer_id
                    WHERE f.sale_date >= '2024-01-01'
                    -- PDS pre-filtering
                    AND EXISTS (SELECT 1 FROM dim_customers c2 WHERE c2.customer_id = f.customer_id)
                    GROUP BY c.region, c.gender
                    HAVING COUNT(f.sale_id) > 0
                    ORDER BY total_revenue DESC
                """,
                "hybrid_query": """
                    SELECT 
                        c.region,
                        c.gender,
                        COUNT(DISTINCT c.customer_id) as unique_customers,
                        SUM(ca.total_spent) as total_revenue,
                        AVG(ca.avg_purchase_amount) as avg_purchase_amount
                    FROM dim_customers c
                    LEFT JOIN mv_customer_analysis_enhanced ca ON c.customer_id = ca.customer_id
                    WHERE ca.first_purchase_date >= '2024-01-01'
                    -- PDS pre-filtering
                    AND EXISTS (SELECT 1 FROM dim_customers c2 WHERE c2.customer_id = ca.customer_id)
                    GROUP BY c.region, c.gender
                    HAVING COUNT(ca.customer_id) > 0
                    ORDER BY total_revenue DESC
                """
            }
        }
    
    def benchmark_hybrid_query(self, test_name: str, query: str, method: str, 
                             description: str, iterations: int = 5) -> Dict[str, Any]:
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
        
        print(f"    Avg: {avg_time:.4f}s, Min: {min_time:.4f}s, Max: {max_time:.4f}s, Rows: {result_rows}")
        
        return result
    
    def generate_hybrid_report(self, results: List[Dict[str, Any]]):
        print("\n" + "="*100)
        print("HYBRID OPTIMIZATION BENCHMARK REPORT")
        print("="*100)
        
        # Group results by test
        grouped_results = {}
        for result in results:
            base_name = '_'.join(result['test_name'].split('_')[:-1])
            if base_name not in grouped_results:
                grouped_results[base_name] = []
            grouped_results[base_name].append(result)
        
        # Generate comparison for each test
        for base_name, test_results in grouped_results.items():
            print(f"\n--- {base_name.upper().replace('_', ' ')} ---")
            print(f"{'Method':<30} {'Avg Time (s)':<12} {'Min Time (s)':<12} {'Max Time (s)':<12} {'Rows':<8} {'Success %':<10}")
            print("-" * 100)
            
            # Sort by average time
            sorted_results = sorted(test_results, key=lambda x: x['avg_time'])
            
            for result in sorted_results:
                print(f"{result['method']:<30} {result['avg_time']:<12.4f} {result['min_time']:<12.4f} "
                      f"{result['max_time']:<12.4f} {result['result_rows']:<8} {result['success_rate']:<10.1f}")
            
            # Calculate speedup and efficiency
            if len(sorted_results) > 1:
                baseline_time = sorted_results[0]['avg_time']
                print(f"\nPerformance Analysis:")
                for i, result in enumerate(sorted_results):
                    if result['avg_time'] != float('inf') and baseline_time != 0:
                        speedup = result['avg_time'] / baseline_time
                        efficiency = (1 / speedup) * 100
                        print(f"  {result['method']}: {speedup:.2f}x time, {efficiency:.1f}% efficiency")
        
        # Overall hybrid effectiveness
        print(f"\n--- HYBRID OPTIMIZATION EFFECTIVENESS ---")
        hybrid_wins = 0
        total_tests = 0
        
        for base_name, test_results in grouped_results.items():
            valid_results = [r for r in test_results if r['avg_time'] != float('inf')]
            if len(valid_results) > 1:
                best_result = min(valid_results, key=lambda x: x['avg_time'])
                if 'hybrid' in best_result['method'].lower():
                    hybrid_wins += 1
                total_tests += 1
        
        if total_tests > 0:
            hybrid_win_rate = (hybrid_wins / total_tests) * 100
            print(f"Hybrid approach wins: {hybrid_wins}/{total_tests} ({hybrid_win_rate:.1f}%)")
            
            if hybrid_win_rate > 50:
                print("✅ Hybrid optimization is highly effective!")
            elif hybrid_win_rate > 25:
                print("⚠️  Hybrid optimization shows moderate effectiveness")
            else:
                print("❌ Hybrid optimization needs improvement")

if __name__ == "__main__":
    from setup_dwh import setup_data_warehouse
    
    # Setup data warehouse
    db, optimizer = setup_data_warehouse()
    
    # Setup hybrid optimization
    hybrid_optimizer = PDSHybridOptimizer(db, optimizer)
    hybrid_optimizer.setup_hybrid_optimization()
    
    # Run hybrid benchmark
    hybrid_optimizer.run_hybrid_benchmark()

