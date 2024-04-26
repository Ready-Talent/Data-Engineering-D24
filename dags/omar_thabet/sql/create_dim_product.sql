CREATE TABLE IF NOT EXISTS data_platform.dim_product (
    product_key serial PRIMARY KEY,
    product_id INTEGER,
    brand_id INTEGER,
    category_id INTEGER,
    name VARCHAR(100),
    price DECIMAL(10, 2),
    description TEXT,
    brand_name VARCHAR(100),
    category_name VARCHAR(100),
    created_by VARCHAR(100) DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100),
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);