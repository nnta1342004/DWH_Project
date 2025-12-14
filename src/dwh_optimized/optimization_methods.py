import pandas as pd
import numpy as np
import time
import hashlib
from typing import List, Dict, Any, Tuple
from collections import defaultdict
import mmh3
import math

class ProbabilisticDataStructures:
    def __init__(self):
        self.bloom_filters = {}
        self.hyperloglog_counters = {}
        self.count_min_sketches = {}
    
    def create_bloom_filter(self, name: str, expected_items: int, false_positive_rate: float = 0.01):
        size = int(-(expected_items * math.log(false_positive_rate)) / (math.log(2) ** 2))
        hash_functions = int((size / expected_items) * math.log(2))
        
        self.bloom_filters[name] = {
            'bit_array': [False] * size,
            'hash_functions': hash_functions,
            'size': size
        }
        return self.bloom_filters[name]
    
    def add_to_bloom_filter(self, name: str, item: str):
        if name not in self.bloom_filters:
            raise ValueError(f"Bloom filter {name} not found")
        
        bf = self.bloom_filters[name]
        for i in range(bf['hash_functions']):
            hash_val = mmh3.hash(item, i) % bf['size']
            bf['bit_array'][hash_val] = True
    
    def check_bloom_filter(self, name: str, item: str) -> bool:
        if name not in self.bloom_filters:
            return False
        
        bf = self.bloom_filters[name]
        for i in range(bf['hash_functions']):
            hash_val = mmh3.hash(item, i) % bf['size']
            if not bf['bit_array'][hash_val]:
                return False
        return True
    
    def create_hyperloglog(self, name: str, precision: int = 4):
        m = 2 ** precision
        self.hyperloglog_counters[name] = {
            'registers': [0] * m,
            'precision': precision,
            'm': m
        }
        return self.hyperloglog_counters[name]
    
    def add_to_hyperloglog(self, name: str, item: str):
        if name not in self.hyperloglog_counters:
            raise ValueError(f"HyperLogLog {name} not found")
        
        hll = self.hyperloglog_counters[name]
        hash_val = mmh3.hash(item)
        binary = format(hash_val, '032b')
        
        j = int(binary[:hll['precision']], 2)
        w = binary[hll['precision']:]
        rho = len(w) - len(w.lstrip('0'))
        
        hll['registers'][j] = max(hll['registers'][j], rho)
    
    def get_hyperloglog_count(self, name: str) -> int:
        if name not in self.hyperloglog_counters:
            return 0
        
        hll = self.hyperloglog_counters[name]
        alpha = 0.7213 / (1 + 1.079 / hll['m'])
        harmonic_mean = hll['m'] / sum(2 ** (-r) for r in hll['registers'])
        return int(alpha * hll['m'] * harmonic_mean)
    
    def create_count_min_sketch(self, name: str, width: int = 1000, depth: int = 5):
        self.count_min_sketches[name] = {
            'sketch': [[0] * width for _ in range(depth)],
            'width': width,
            'depth': depth
        }
        return self.count_min_sketches[name]
    
    def add_to_count_min_sketch(self, name: str, item: str, count: int = 1):
        if name not in self.count_min_sketches:
            raise ValueError(f"Count-Min Sketch {name} not found")
        
        cm = self.count_min_sketches[name]
        for i in range(cm['depth']):
            hash_val = mmh3.hash(item, i) % cm['width']
            cm['sketch'][i][hash_val] += count
    
    def get_count_min_sketch_count(self, name: str, item: str) -> int:
        if name not in self.count_min_sketches:
            return 0
        
        cm = self.count_min_sketches[name]
        min_count = float('inf')
        for i in range(cm['depth']):
            hash_val = mmh3.hash(item, i) % cm['width']
            min_count = min(min_count, cm['sketch'][i][hash_val])
        return int(min_count)

class QueryOptimizer:
    def __init__(self, db_connection):
        self.db = db_connection
        self.pds = ProbabilisticDataStructures()
        self.materialized_views = {}
        self.indexes = {}
    
    def create_materialized_view(self, view_name: str, query: str):
        create_mv_query = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} AS
        {query}
        """
        self.db.execute_command(create_mv_query)
        self.materialized_views[view_name] = query
        print(f"Created materialized view: {view_name}")
    
    def refresh_materialized_view(self, view_name: str):
        refresh_query = f"REFRESH MATERIALIZED VIEW {view_name}"
        self.db.execute_command(refresh_query)
        print(f"Refreshed materialized view: {view_name}")
    
    def create_index(self, table_name: str, column_name: str, index_name: str = None):
        if not index_name:
            index_name = f"idx_{table_name}_{column_name}"
        
        create_index_query = f"""
        CREATE INDEX IF NOT EXISTS {index_name} 
        ON {table_name} ({column_name})
        """
        self.db.execute_command(create_index_query)
        self.indexes[f"{table_name}_{column_name}"] = index_name
        print(f"Created index: {index_name}")
    
    def create_partial_index(self, table_name: str, column_name: str, condition: str, index_name: str = None):
        if not index_name:
            index_name = f"idx_{table_name}_{column_name}_partial"
        
        create_index_query = f"""
        CREATE INDEX IF NOT EXISTS {index_name} 
        ON {table_name} ({column_name})
        WHERE {condition}
        """
        self.db.execute_command(create_index_query)
        print(f"Created partial index: {index_name}")
    
    def setup_bloom_filter_for_table(self, table_name: str, column_name: str, 
                                   expected_items: int = 10000):
        query = f"SELECT DISTINCT {column_name} FROM {table_name}"
        df = self.db.execute_query(query)
        
        bf_name = f"{table_name}_{column_name}_bf"
        self.pds.create_bloom_filter(bf_name, expected_items)
        
        for value in df[column_name].dropna():
            self.pds.add_to_bloom_filter(bf_name, str(value))
        
        print(f"Created Bloom filter for {table_name}.{column_name}")
        return bf_name
    
    def setup_hyperloglog_for_table(self, table_name: str, column_name: str):
        query = f"SELECT {column_name} FROM {table_name}"
        df = self.db.execute_query(query)
        
        hll_name = f"{table_name}_{column_name}_hll"
        self.pds.create_hyperloglog(hll_name)
        
        for value in df[column_name].dropna():
            self.pds.add_to_hyperloglog(hll_name, str(value))
        
        estimated_count = self.pds.get_hyperloglog_count(hll_name)
        actual_count = df[column_name].nunique()
        error_rate = abs(estimated_count - actual_count) / actual_count * 100
        
        print(f"Created HyperLogLog for {table_name}.{column_name}")
        print(f"Estimated distinct count: {estimated_count}, Actual: {actual_count}, Error: {error_rate:.2f}%")
        return hll_name
    
    def setup_count_min_sketch_for_table(self, table_name: str, column_name: str):
        query = f"SELECT {column_name}, COUNT(*) as cnt FROM {table_name} GROUP BY {column_name}"
        df = self.db.execute_query(query)
        
        cm_name = f"{table_name}_{column_name}_cm"
        self.pds.create_count_min_sketch(cm_name)
        
        for _, row in df.iterrows():
            self.pds.add_to_count_min_sketch(cm_name, str(row[column_name]), int(row['cnt']))
        
        print(f"Created Count-Min Sketch for {table_name}.{column_name}")
        return cm_name
    
    def benchmark_query(self, query: str, method_name: str, iterations: int = 5) -> Dict[str, float]:
        times = []
        
        for i in range(iterations):
            start_time = time.time()
            result = self.db.execute_query(query)
            end_time = time.time()
            times.append(end_time - start_time)
        
        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)
        
        return {
            'method': method_name,
            'avg_time': avg_time,
            'min_time': min_time,
            'max_time': max_time,
            'iterations': iterations,
            'result_rows': len(result)
        }

