import os
import sys
from db_connection import DatabaseConnection
from optimization_methods import QueryOptimizer

def setup_data_warehouse():
    print("=== Setting up Optimized Data Warehouse ===")
    
    # Initialize database connection
    db = DatabaseConnection()
    
    # Create optimized tables
    create_optimized_tables(db)
    
    # Load sample data
    load_sample_data(db)
    
    # Initialize optimizer
    optimizer = QueryOptimizer(db)
    
    # Setup optimization methods
    setup_optimization_methods(optimizer)
    
    print("=== Data Warehouse Setup Complete ===")
    return db, optimizer

def create_optimized_tables(db):
    print("\n--- Creating Optimized Tables ---")
    
    # Drop existing tables if they exist
    drop_tables = [
        "DROP TABLE IF EXISTS fact_sales CASCADE",
        "DROP TABLE IF EXISTS dim_customers CASCADE", 
        "DROP TABLE IF EXISTS dim_products CASCADE",
        "DROP TABLE IF EXISTS dim_locations CASCADE",
        "DROP TABLE IF EXISTS dim_categories CASCADE"
    ]
    
    for query in drop_tables:
        db.execute_command(query)
    
    # Create dimension tables
    create_dim_customers = """
    CREATE TABLE dim_customers (
        customer_id VARCHAR(50) PRIMARY KEY,
        customer_name VARCHAR(255),
        birth_date DATE,
        gender VARCHAR(10),
        email VARCHAR(255),
        phone VARCHAR(20),
        address TEXT,
        city VARCHAR(100),
        region VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    create_dim_products = """
    CREATE TABLE dim_products (
        product_id VARCHAR(50) PRIMARY KEY,
        product_name VARCHAR(255),
        category_id VARCHAR(50),
        category_name VARCHAR(100),
        unit_price DECIMAL(10,2),
        cost DECIMAL(10,2),
        description TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    create_dim_locations = """
    CREATE TABLE dim_locations (
        location_id VARCHAR(50) PRIMARY KEY,
        location_name VARCHAR(255),
        city VARCHAR(100),
        region VARCHAR(100),
        country VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    create_dim_categories = """
    CREATE TABLE dim_categories (
        category_id VARCHAR(50) PRIMARY KEY,
        category_name VARCHAR(100),
        parent_category_id VARCHAR(50),
        description TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    # Create fact table with partitioning
    create_fact_sales = """
    CREATE TABLE fact_sales (
        sale_id VARCHAR(50) PRIMARY KEY,
        customer_id VARCHAR(50),
        product_id VARCHAR(50),
        location_id VARCHAR(50),
        sale_date DATE,
        quantity INTEGER,
        unit_price DECIMAL(10,2),
        total_amount DECIMAL(12,2),
        discount_amount DECIMAL(10,2),
        net_amount DECIMAL(12,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
        FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
        FOREIGN KEY (location_id) REFERENCES dim_locations(location_id)
    ) PARTITION BY RANGE (sale_date)
    """
    
    # Create partitions for fact_sales
    create_partitions = [
        "CREATE TABLE fact_sales_2023 PARTITION OF fact_sales FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')",
        "CREATE TABLE fact_sales_2024 PARTITION OF fact_sales FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')",
        "CREATE TABLE fact_sales_2025 PARTITION OF fact_sales FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')"
    ]
    
    tables = [
        create_dim_customers,
        create_dim_products, 
        create_dim_locations,
        create_dim_categories,
        create_fact_sales
    ]
    
    for query in tables:
        db.execute_command(query)
        print(f"Created table: {query.split()[2]}")
    
    for query in create_partitions:
        db.execute_command(query)
        print(f"Created partition: {query.split()[2]}")
    
    print("--- Tables Created Successfully ---")

def load_sample_data(db):
    print("\n--- Loading Sample Data ---")
    
    # Load customers
    customers_data = [
        ('CUST001', 'John Doe', '1990-05-15', 'Male', 'john@email.com', '1234567890', '123 Main St', 'New York', 'NY'),
        ('CUST002', 'Jane Smith', '1985-08-22', 'Female', 'jane@email.com', '0987654321', '456 Oak Ave', 'Los Angeles', 'CA'),
        ('CUST003', 'Bob Johnson', '1992-03-10', 'Male', 'bob@email.com', '1122334455', '789 Pine St', 'Chicago', 'IL'),
        ('CUST004', 'Alice Brown', '1988-12-05', 'Female', 'alice@email.com', '5566778899', '321 Elm St', 'Houston', 'TX'),
        ('CUST005', 'Charlie Wilson', '1995-07-18', 'Male', 'charlie@email.com', '9988776655', '654 Maple Ave', 'Phoenix', 'AZ')
    ]
    
    for customer in customers_data:
        db.execute_command("""
            INSERT INTO dim_customers (customer_id, customer_name, birth_date, gender, email, phone, address, city, region)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, customer)
    
    # Load products
    products_data = [
        ('PROD001', 'Laptop Pro', 'CAT001', 'Electronics', 1299.99, 800.00, 'High-performance laptop'),
        ('PROD002', 'Wireless Mouse', 'CAT001', 'Electronics', 29.99, 15.00, 'Ergonomic wireless mouse'),
        ('PROD003', 'Office Chair', 'CAT002', 'Furniture', 199.99, 120.00, 'Comfortable office chair'),
        ('PROD004', 'Coffee Maker', 'CAT003', 'Appliances', 89.99, 50.00, 'Automatic coffee maker'),
        ('PROD005', 'Desk Lamp', 'CAT002', 'Furniture', 49.99, 25.00, 'LED desk lamp')
    ]
    
    for product in products_data:
        db.execute_command("""
            INSERT INTO dim_products (product_id, product_name, category_id, category_name, unit_price, cost, description)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, product)
    
    # Load locations
    locations_data = [
        ('LOC001', 'NYC Store', 'New York', 'NY', 'USA'),
        ('LOC002', 'LA Store', 'Los Angeles', 'CA', 'USA'),
        ('LOC003', 'Chicago Store', 'Chicago', 'IL', 'USA'),
        ('LOC004', 'Houston Store', 'Houston', 'TX', 'USA'),
        ('LOC005', 'Phoenix Store', 'Phoenix', 'AZ', 'USA')
    ]
    
    for location in locations_data:
        db.execute_command("""
            INSERT INTO dim_locations (location_id, location_name, city, region, country)
            VALUES (%s, %s, %s, %s, %s)
        """, location)
    
    # Load categories
    categories_data = [
        ('CAT001', 'Electronics', None, 'Electronic devices and accessories'),
        ('CAT002', 'Furniture', None, 'Office and home furniture'),
        ('CAT003', 'Appliances', None, 'Home and kitchen appliances')
    ]
    
    for category in categories_data:
        db.execute_command("""
            INSERT INTO dim_categories (category_id, category_name, parent_category_id, description)
            VALUES (%s, %s, %s, %s)
        """, category)
    
    # Generate sample sales data
    import random
    from datetime import datetime, timedelta
    
    sales_data = []
    for i in range(1000):
        sale_id = f'SALE{i+1:06d}'
        customer_id = f'CUST{random.randint(1, 5):03d}'
        product_id = f'PROD{random.randint(1, 5):03d}'
        location_id = f'LOC{random.randint(1, 5):03d}'
        sale_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 364))
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(20, 1500), 2)
        total_amount = round(quantity * unit_price, 2)
        discount_amount = round(total_amount * random.uniform(0, 0.2), 2)
        net_amount = round(total_amount - discount_amount, 2)
        
        sales_data.append((
            sale_id, customer_id, product_id, location_id, sale_date,
            quantity, unit_price, total_amount, discount_amount, net_amount
        ))
    
    for sale in sales_data:
        db.execute_command("""
            INSERT INTO fact_sales (sale_id, customer_id, product_id, location_id, sale_date, 
                                  quantity, unit_price, total_amount, discount_amount, net_amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, sale)
    
    print("--- Sample Data Loaded Successfully ---")

def setup_optimization_methods(optimizer):
    print("\n--- Setting up Optimization Methods ---")
    
    # Create materialized views for common aggregations
    optimizer.create_materialized_view(
        "mv_daily_sales",
        """
        SELECT 
            sale_date,
            COUNT(*) as total_sales,
            SUM(quantity) as total_quantity,
            SUM(net_amount) as total_revenue,
            AVG(net_amount) as avg_sale_amount
        FROM fact_sales 
        GROUP BY sale_date
        ORDER BY sale_date
        """
    )
    
    optimizer.create_materialized_view(
        "mv_customer_sales",
        """
        SELECT 
            c.customer_id,
            c.customer_name,
            c.region,
            COUNT(f.sale_id) as total_purchases,
            SUM(f.net_amount) as total_spent,
            AVG(f.net_amount) as avg_purchase_amount
        FROM dim_customers c
        LEFT JOIN fact_sales f ON c.customer_id = f.customer_id
        GROUP BY c.customer_id, c.customer_name, c.region
        """
    )
    
    optimizer.create_materialized_view(
        "mv_product_performance",
        """
        SELECT 
            p.product_id,
            p.product_name,
            p.category_name,
            COUNT(f.sale_id) as total_sales,
            SUM(f.quantity) as total_quantity_sold,
            SUM(f.net_amount) as total_revenue,
            AVG(f.unit_price) as avg_price
        FROM dim_products p
        LEFT JOIN fact_sales f ON p.product_id = f.product_id
        GROUP BY p.product_id, p.product_name, p.category_name
        """
    )
    
    # Create indexes for common query patterns
    optimizer.create_index("fact_sales", "sale_date")
    optimizer.create_index("fact_sales", "customer_id")
    optimizer.create_index("fact_sales", "product_id")
    optimizer.create_index("fact_sales", "location_id")
    optimizer.create_index("dim_customers", "region")
    optimizer.create_index("dim_products", "category_id")
    
    # Create partial indexes for recent data
    optimizer.create_partial_index(
        "fact_sales", "sale_date", 
        "sale_date >= '2024-01-01'", 
        "idx_fact_sales_recent"
    )
    
    # Setup Probabilistic Data Structures
    print("\n--- Setting up Probabilistic Data Structures ---")
    
    # Bloom filters for existence checks
    optimizer.setup_bloom_filter_for_table("dim_customers", "customer_id")
    optimizer.setup_bloom_filter_for_table("dim_products", "product_id")
    optimizer.setup_bloom_filter_for_table("fact_sales", "sale_id")
    
    # HyperLogLog for distinct count estimation
    optimizer.setup_hyperloglog_for_table("fact_sales", "customer_id")
    optimizer.setup_hyperloglog_for_table("fact_sales", "product_id")
    
    # Count-Min Sketch for frequency estimation
    optimizer.setup_count_min_sketch_for_table("fact_sales", "customer_id")
    optimizer.setup_count_min_sketch_for_table("fact_sales", "product_id")
    
    print("--- Optimization Methods Setup Complete ---")

if __name__ == "__main__":
    db, optimizer = setup_data_warehouse()
    print("\nData Warehouse setup completed successfully!")

