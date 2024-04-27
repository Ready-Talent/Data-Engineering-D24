CREATE TABLE IF NOT EXISTS data_platform_01.dim_product (
    product_key INTEGER,
    product_id INTEGER,
    brand_id INTEGER,
    category_id INTEGER,
    name STRING,
    price NUMERIC(10, 2),
    description STRING,
    brand_name STRING,
    category_name STRING,
    created_by STRING DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_by STRING,
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
