-- Connect to your database
\c ecommerse_db;

-- 1. RAW ORDERS TABLE (from Kafka)
-- This stores all incoming orders as they arrive
CREATE TABLE IF NOT EXISTS orders_raw (
    id SERIAL PRIMARY KEY,
    order_id INTEGER,
    user_id INTEGER NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    kafka_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. PROCESSED ORDERS TABLE (cleaned and deduplicated)
-- This is where Spark writes processed/aggregated data
CREATE TABLE IF NOT EXISTS orders_processed (
    id SERIAL PRIMARY KEY,
    order_id INTEGER UNIQUE,
    user_id INTEGER NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    order_date TIMESTAMP NOT NULL,
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Additional computed fields
    price_category VARCHAR(20), -- 'low', 'medium', 'high'
    is_valid BOOLEAN DEFAULT true
);

-- 3. PRODUCT ANALYTICS (aggregated by Spark)
CREATE TABLE IF NOT EXISTS product_analytics (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) UNIQUE NOT NULL,
    total_orders INTEGER DEFAULT 0,
    total_sales DECIMAL(12, 2) DEFAULT 0,
    avg_price DECIMAL(10, 2) DEFAULT 0,
    max_price DECIMAL(10, 2) DEFAULT 0,
    min_price DECIMAL(10, 2) DEFAULT 0,
    last_order_date TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. USER ANALYTICS (aggregated by Spark)
CREATE TABLE IF NOT EXISTS user_analytics (
    id SERIAL PRIMARY KEY,
    user_id INTEGER UNIQUE NOT NULL,
    total_orders INTEGER DEFAULT 0,
    total_spent DECIMAL(12, 2) DEFAULT 0,
    avg_order_value DECIMAL(10, 2) DEFAULT 0,
    first_order_date TIMESTAMP,
    last_order_date TIMESTAMP,
    user_segment VARCHAR(20), -- 'new', 'regular', 'vip'
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. HOURLY SALES SUMMARY (for time-series charts)
CREATE TABLE IF NOT EXISTS hourly_sales (
    id SERIAL PRIMARY KEY,
    hour_timestamp TIMESTAMP NOT NULL,
    total_orders INTEGER DEFAULT 0,
    total_revenue DECIMAL(12, 2) DEFAULT 0,
    unique_users INTEGER DEFAULT 0,
    avg_order_value DECIMAL(10, 2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(hour_timestamp)
);

-- 6. DAILY SALES SUMMARY
CREATE TABLE IF NOT EXISTS daily_sales (
    id SERIAL PRIMARY KEY,
    sale_date DATE NOT NULL,
    total_orders INTEGER DEFAULT 0,
    total_revenue DECIMAL(12, 2) DEFAULT 0,
    unique_users INTEGER DEFAULT 0,
    unique_products INTEGER DEFAULT 0,
    avg_order_value DECIMAL(10, 2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(sale_date)
);

-- 7. PRODUCT PERFORMANCE (real-time metrics)
CREATE TABLE IF NOT EXISTS product_performance (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    time_window VARCHAR(20) NOT NULL, -- '1hour', '24hour', '7days'
    orders_count INTEGER DEFAULT 0,
    revenue DECIMAL(12, 2) DEFAULT 0,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(product_name, time_window)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_orders_raw_user ON orders_raw(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_raw_product ON orders_raw(product_name);
CREATE INDEX IF NOT EXISTS idx_orders_raw_timestamp ON orders_raw(kafka_timestamp);
CREATE INDEX IF NOT EXISTS idx_orders_raw_created ON orders_raw(created_at);

CREATE INDEX IF NOT EXISTS idx_orders_processed_user ON orders_processed(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_processed_product ON orders_processed(product_name);
CREATE INDEX IF NOT EXISTS idx_orders_processed_date ON orders_processed(order_date);

CREATE INDEX IF NOT EXISTS idx_hourly_sales_timestamp ON hourly_sales(hour_timestamp);
CREATE INDEX IF NOT EXISTS idx_daily_sales_date ON daily_sales(sale_date);

-- Create views for dashboard
CREATE OR REPLACE VIEW dashboard_summary AS
SELECT 
    COUNT(*) as total_orders,
    COUNT(DISTINCT user_id) as total_users,
    COUNT(DISTINCT product_name) as total_products,
    SUM(price) as total_revenue,
    AVG(price) as avg_order_value,
    MAX(created_at) as last_order_time
FROM orders_raw;

CREATE OR REPLACE VIEW recent_orders_view AS
SELECT 
    order_id,
    user_id,
    product_name,
    price,
    kafka_timestamp,
    created_at
FROM orders_raw
ORDER BY created_at DESC
LIMIT 100;

CREATE OR REPLACE VIEW top_products AS
SELECT 
    product_name,
    total_orders,
    total_sales,
    avg_price
FROM product_analytics
ORDER BY total_sales DESC
LIMIT 10;

CREATE OR REPLACE VIEW top_users AS
SELECT 
    user_id,
    total_orders,
    total_spent,
    avg_order_value,
    user_segment
FROM user_analytics
ORDER BY total_spent DESC
LIMIT 10;

-- Grant permissions (adjust username as needed)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;