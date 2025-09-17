-- Create database schema for ETL demo
CREATE SCHEMA IF NOT EXISTS etl_demo;

-- Create source tables for CDC
CREATE TABLE IF NOT EXISTS etl_demo.customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(50),
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_demo.products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10,2) NOT NULL,
    stock_quantity INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_demo.orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES etl_demo.customers(id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_demo.order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES etl_demo.orders(id),
    product_id INTEGER REFERENCES etl_demo.products(id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create analytics tables for batch processing
CREATE TABLE IF NOT EXISTS etl_demo.customer_segments (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES etl_demo.customers(id),
    segment VARCHAR(100),
    total_orders INTEGER DEFAULT 0,
    total_spent DECIMAL(15,2) DEFAULT 0,
    avg_order_value DECIMAL(10,2) DEFAULT 0,
    last_order_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(customer_id)
);

CREATE TABLE IF NOT EXISTS etl_demo.product_analytics (
    id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES etl_demo.products(id),
    date_analyzed DATE DEFAULT CURRENT_DATE,
    units_sold INTEGER DEFAULT 0,
    revenue DECIMAL(15,2) DEFAULT 0,
    avg_selling_price DECIMAL(10,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, date_analyzed)
);

CREATE TABLE IF NOT EXISTS etl_demo.revenue_summary (
    id SERIAL PRIMARY KEY,
    date_analyzed DATE DEFAULT CURRENT_DATE,
    total_revenue DECIMAL(15,2) DEFAULT 0,
    total_orders INTEGER DEFAULT 0,
    avg_order_value DECIMAL(10,2) DEFAULT 0,
    unique_customers INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date_analyzed)
);

-- Create triggers for updated_at timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_customers_updated_at BEFORE UPDATE ON etl_demo.customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_products_updated_at BEFORE UPDATE ON etl_demo.products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_orders_updated_at BEFORE UPDATE ON etl_demo.orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_customer_segments_updated_at BEFORE UPDATE ON etl_demo.customer_segments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_customers_email ON etl_demo.customers(email);
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON etl_demo.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_date ON etl_demo.orders(order_date);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON etl_demo.order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON etl_demo.order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_product_analytics_date ON etl_demo.product_analytics(date_analyzed);
CREATE INDEX IF NOT EXISTS idx_revenue_summary_date ON etl_demo.revenue_summary(date_analyzed);

-- Insert some initial test data
INSERT INTO etl_demo.customers (name, email, phone, address) VALUES
('John Doe', 'john.doe@example.com', '+1-555-0123', '123 Main St, Anytown, USA'),
('Jane Smith', 'jane.smith@example.com', '+1-555-0124', '456 Oak Ave, Somewhere, USA'),
('Bob Johnson', 'bob.johnson@example.com', '+1-555-0125', '789 Pine Rd, Elsewhere, USA')
ON CONFLICT (email) DO NOTHING;

INSERT INTO etl_demo.products (name, category, price, stock_quantity) VALUES
('Laptop Pro', 'Electronics', 1299.99, 50),
('Wireless Mouse', 'Electronics', 29.99, 200),
('Office Chair', 'Furniture', 249.99, 30),
('Coffee Mug', 'Kitchen', 12.99, 100),
('Notebook', 'Stationery', 8.99, 150)
ON CONFLICT DO NOTHING;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA etl_demo TO etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA etl_demo TO etl_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA etl_demo TO etl_user;