import time
import pandas as pd
from db_connection import DatabaseConnection
from optimization_methods import QueryOptimizer

class PDSDemo:
    def __init__(self):
        self.db = DatabaseConnection()
        self.optimizer = QueryOptimizer(self.db)
        self.pds = self.optimizer.pds
    
    def run_pds_demonstration(self):
        """Demonstrate the power of Probabilistic Data Structures"""
        print("="*80)
        print("PROBABILISTIC DATA STRUCTURES DEMONSTRATION")
        print("="*80)
        print("This demo shows the speed and efficiency of PDS compared to traditional methods")
        print()
        
        # Setup PDS structures
        self.setup_pds_structures()
        
        # Run demonstrations
        self.demonstrate_bloom_filter()
        self.demonstrate_hyperloglog()
        self.demonstrate_count_min_sketch()
        self.demonstrate_combined_pds()
        
        print("\n" + "="*80)
        print("PDS DEMONSTRATION COMPLETE")
        print("="*80)
    
    def setup_pds_structures(self):
        """Setup PDS structures for demonstration"""
        print("Setting up Probabilistic Data Structures...")
        
        # Setup Bloom filters
        self.optimizer.setup_bloom_filter_for_table("dim_customers", "customer_id", 1000)
        self.optimizer.setup_bloom_filter_for_table("dim_products", "product_id", 1000)
        self.optimizer.setup_bloom_filter_for_table("fact_sales", "sale_id", 10000)
        
        # Setup HyperLogLog
        self.optimizer.setup_hyperloglog_for_table("fact_sales", "customer_id")
        self.optimizer.setup_hyperloglog_for_table("fact_sales", "product_id")
        
        # Setup Count-Min Sketch
        self.optimizer.setup_count_min_sketch_for_table("fact_sales", "customer_id")
        self.optimizer.setup_count_min_sketch_for_table("fact_sales", "product_id")
        
        print("✓ PDS structures ready")
        print()
    
    def demonstrate_bloom_filter(self):
        """Demonstrate Bloom Filter speed and accuracy"""
        print("--- BLOOM FILTER DEMONSTRATION ---")
        print("Testing existence checks with Bloom Filter vs SQL queries")
        print()
        
        # Test values
        test_customers = ['CUST001', 'CUST002', 'CUST999', 'CUST888']
        test_products = ['PROD001', 'PROD002', 'PROD999', 'PROD888']
        
        print("Customer existence checks:")
        print(f"{'Customer ID':<12} {'Bloom Filter':<15} {'SQL Query':<15} {'Match':<8} {'Speedup':<10}")
        print("-" * 70)
        
        for customer in test_customers:
            # Bloom Filter check
            bf_start = time.time()
            bf_exists = self.pds.check_bloom_filter("dim_customers_customer_id_bf", customer)
            bf_time = time.time() - bf_start
            
            # SQL query check
            sql_start = time.time()
            sql_result = self.db.execute_query(
                "SELECT COUNT(*) FROM dim_customers WHERE customer_id = %s", 
                (customer,)
            )
            sql_exists = sql_result.iloc[0, 0] > 0
            sql_time = time.time() - sql_start
            
            match = "✓" if bf_exists == sql_exists else "✗"
            speedup = sql_time / bf_time if bf_time > 0 else float('inf')
            
            print(f"{customer:<12} {bf_exists!s:<15} {sql_exists!s:<15} {match:<8} {speedup:<10.1f}x")
        
        print()
        print("Product existence checks:")
        print(f"{'Product ID':<12} {'Bloom Filter':<15} {'SQL Query':<15} {'Match':<8} {'Speedup':<10}")
        print("-" * 70)
        
        for product in test_products:
            # Bloom Filter check
            bf_start = time.time()
            bf_exists = self.pds.check_bloom_filter("dim_products_product_id_bf", product)
            bf_time = time.time() - bf_start
            
            # SQL query check
            sql_start = time.time()
            sql_result = self.db.execute_query(
                "SELECT COUNT(*) FROM dim_products WHERE product_id = %s", 
                (product,)
            )
            sql_exists = sql_result.iloc[0, 0] > 0
            sql_time = time.time() - sql_start
            
            match = "✓" if bf_exists == sql_exists else "✗"
            speedup = sql_time / bf_time if bf_time > 0 else float('inf')
            
            print(f"{product:<12} {bf_exists!s:<15} {sql_exists!s:<15} {match:<8} {speedup:<10.1f}x")
        
        print()
        print("🎯 Bloom Filter Benefits:")
        print("  • Near-instant existence checks")
        print("  • Minimal memory usage")
        print("  • No false negatives (only false positives)")
        print("  • Perfect for pre-filtering before expensive operations")
        print()
    
    def demonstrate_hyperloglog(self):
        """Demonstrate HyperLogLog speed and accuracy"""
        print("--- HYPERLOGLOG DEMONSTRATION ---")
        print("Testing distinct count estimation with HyperLogLog vs SQL queries")
        print()
        
        # Test different data subsets
        test_cases = [
            ("All customers", "SELECT DISTINCT customer_id FROM fact_sales"),
            ("All products", "SELECT DISTINCT product_id FROM fact_sales"),
            ("Recent sales", "SELECT DISTINCT customer_id FROM fact_sales WHERE sale_date >= '2024-01-01'"),
            ("High-value sales", "SELECT DISTINCT customer_id FROM fact_sales WHERE net_amount > 100")
        ]
        
        print(f"{'Test Case':<20} {'HLL Estimate':<15} {'SQL Actual':<15} {'Error %':<10} {'Speedup':<10}")
        print("-" * 80)
        
        for test_name, sql_query in test_cases:
            # HyperLogLog estimation
            hll_start = time.time()
            if "customer_id" in sql_query:
                hll_count = self.pds.get_hyperloglog_count("fact_sales_customer_id_hll")
            else:
                hll_count = self.pds.get_hyperloglog_count("fact_sales_product_id_hll")
            hll_time = time.time() - hll_start
            
            # SQL actual count
            sql_start = time.time()
            sql_result = self.db.execute_query(sql_query)
            sql_count = len(sql_result)
            sql_time = time.time() - sql_start
            
            error_rate = abs(hll_count - sql_count) / max(sql_count, 1) * 100
            speedup = sql_time / hll_time if hll_time > 0 else float('inf')
            
            print(f"{test_name:<20} {hll_count:<15} {sql_count:<15} {error_rate:<10.2f} {speedup:<10.1f}x")
        
        print()
        print("🎯 HyperLogLog Benefits:")
        print("  • Constant time complexity O(1)")
        print("  • Minimal memory usage")
        print("  • High accuracy for large datasets")
        print("  • Perfect for real-time analytics")
        print()
    
    def demonstrate_count_min_sketch(self):
        """Demonstrate Count-Min Sketch speed and accuracy"""
        print("--- COUNT-MIN SKETCH DEMONSTRATION ---")
        print("Testing frequency estimation with Count-Min Sketch vs SQL queries")
        print()
        
        # Test different customers
        test_customers = ['CUST001', 'CUST002', 'CUST003', 'CUST004', 'CUST005']
        
        print(f"{'Customer ID':<12} {'CM Estimate':<15} {'SQL Actual':<15} {'Error %':<10} {'Speedup':<10}")
        print("-" * 80)
        
        for customer in test_customers:
            # Count-Min Sketch estimation
            cm_start = time.time()
            cm_count = self.pds.get_count_min_sketch_count("fact_sales_customer_id_cm", customer)
            cm_time = time.time() - cm_start
            
            # SQL actual count
            sql_start = time.time()
            sql_result = self.db.execute_query(
                "SELECT COUNT(*) FROM fact_sales WHERE customer_id = %s", 
                (customer,)
            )
            sql_count = sql_result.iloc[0, 0]
            sql_time = time.time() - sql_start
            
            error_rate = abs(cm_count - sql_count) / max(sql_count, 1) * 100
            speedup = sql_time / cm_time if cm_time > 0 else float('inf')
            
            print(f"{customer:<12} {cm_count:<15} {sql_count:<15} {error_rate:<10.2f} {speedup:<10.1f}x")
        
        print()
        print("🎯 Count-Min Sketch Benefits:")
        print("  • Fast frequency estimation")
        print("  • Memory efficient")
        print("  • Good accuracy for high-frequency items")
        print("  • Perfect for trend analysis")
        print()
    
    def demonstrate_combined_pds(self):
        """Demonstrate combined PDS usage for complex queries"""
        print("--- COMBINED PDS DEMONSTRATION ---")
        print("Testing complex query optimization using multiple PDS structures")
        print()
        
        # Complex query: Find customers who bought specific products
        target_customers = ['CUST001', 'CUST002', 'CUST999']
        target_products = ['PROD001', 'PROD002', 'PROD999']
        
        print("Complex Query: Find customers who bought specific products")
        print(f"Target customers: {target_customers}")
        print(f"Target products: {target_products}")
        print()
        
        # Method 1: Traditional SQL approach
        print("Method 1: Traditional SQL (full table scan)")
        sql_start = time.time()
        
        # Build SQL query
        customer_conditions = "', '".join(target_customers)
        product_conditions = "', '".join(target_products)
        sql_query = f"""
        SELECT DISTINCT f.customer_id, f.product_id
        FROM fact_sales f
        WHERE f.customer_id IN ('{customer_conditions}')
        AND f.product_id IN ('{product_conditions}')
        """
        
        sql_result = self.db.execute_query(sql_query)
        sql_time = time.time() - sql_start
        
        print(f"SQL Result: {len(sql_result)} combinations found in {sql_time:.4f}s")
        
        # Method 2: PDS pre-filtering approach
        print("\nMethod 2: PDS Pre-filtering (Bloom Filter + Count-Min Sketch)")
        pds_start = time.time()
        
        # Pre-filter using Bloom Filters
        valid_customers = []
        for customer in target_customers:
            if self.pds.check_bloom_filter("dim_customers_customer_id_bf", customer):
                valid_customers.append(customer)
        
        valid_products = []
        for product in target_products:
            if self.pds.check_bloom_filter("dim_products_product_id_bf", product):
                valid_products.append(product)
        
        # Get frequency estimates using Count-Min Sketch
        pds_combinations = []
        for customer in valid_customers:
            for product in valid_products:
                customer_freq = self.pds.get_count_min_sketch_count("fact_sales_customer_id_cm", customer)
                product_freq = self.pds.get_count_min_sketch_count("fact_sales_product_id_cm", product)
                
                # Estimate if combination exists (simplified heuristic)
                if customer_freq > 0 and product_freq > 0:
                    pds_combinations.append((customer, product))
        
        pds_time = time.time() - pds_start
        
        print(f"PDS Result: {len(pds_combinations)} combinations estimated in {pds_time:.4f}s")
        print(f"Valid customers after BF: {valid_customers}")
        print(f"Valid products after BF: {valid_products}")
        
        # Calculate accuracy and speedup
        accuracy = len(set(pds_combinations) & set(zip(sql_result['customer_id'], sql_result['product_id']))) / len(sql_result) * 100
        speedup = sql_time / pds_time if pds_time > 0 else float('inf')
        
        print(f"\nAccuracy: {accuracy:.1f}%")
        print(f"Speedup: {speedup:.1f}x")
        
        print()
        print("🎯 Combined PDS Benefits:")
        print("  • Multi-stage filtering reduces data volume")
        print("  • Approximate results in constant time")
        print("  • Perfect for real-time dashboards")
        print("  • Scales to massive datasets")
        print()
    
    def demonstrate_memory_efficiency(self):
        """Demonstrate memory efficiency of PDS"""
        print("--- MEMORY EFFICIENCY DEMONSTRATION ---")
        print("Comparing memory usage of PDS vs traditional approaches")
        print()
        
        # Calculate PDS memory usage
        bf_memory = 0
        for name, bf in self.pds.bloom_filters.items():
            bf_memory += bf['size'] / 8  # bits to bytes
        
        hll_memory = 0
        for name, hll in self.pds.hyperloglog_counters.items():
            hll_memory += hll['m'] * 4  # 4 bytes per register
        
        cm_memory = 0
        for name, cm in self.pds.count_min_sketches.items():
            cm_memory += cm['width'] * cm['depth'] * 4  # 4 bytes per counter
        
        total_pds_memory = bf_memory + hll_memory + cm_memory
        
        # Estimate traditional approach memory
        # (This is a rough estimate for demonstration)
        total_records = 1000  # Approximate
        traditional_memory = total_records * 100  # 100 bytes per record estimate
        
        print(f"PDS Memory Usage:")
        print(f"  Bloom Filters: {bf_memory:.2f} bytes")
        print(f"  HyperLogLog: {hll_memory:.2f} bytes")
        print(f"  Count-Min Sketch: {cm_memory:.2f} bytes")
        print(f"  Total PDS: {total_pds_memory:.2f} bytes")
        print()
        print(f"Traditional Approach (estimated): {traditional_memory:.2f} bytes")
        print(f"Memory Efficiency: {traditional_memory / total_pds_memory:.1f}x less memory")
        print()
        print("🎯 Memory Efficiency Benefits:")
        print("  • Constant memory usage regardless of data size")
        print("  • Fits in CPU cache for maximum speed")
        print("  • Enables real-time processing of massive datasets")
        print()

if __name__ == "__main__":
    demo = PDSDemo()
    demo.run_pds_demonstration()
    demo.demonstrate_memory_efficiency()

