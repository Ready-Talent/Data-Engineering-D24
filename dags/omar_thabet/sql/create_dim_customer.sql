CREATE TABLE IF NOT EXISTS data_platform.dim_customer (
    customer_id INTEGER,
    customer_name STRING,
    email STRING,
    phone STRING,
    address_id INTEGER,
    address_street STRING,
    address_zipcode STRING,
    city_id INTEGER,
    city_name STRING,
    state_id INTEGER,
    state_name STRING,
    country_id INTEGER,
    country_name STRING,
    created_by STRING DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_by STRING,
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
