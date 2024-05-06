-- create dim_date
CREATE TABLE IF NOT EXISTS Data_Platform_Lojain.dim_product (
    product_key INT64 GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    product_id INT64,
    brand_id INT64,
    category_id INT64,
    name STRING,
    price NUMERIC(10, 2),
    description STRING,
    brand_name STRING,
    category_name STRING,
    created_by STRING DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    modified_by STRING,
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

