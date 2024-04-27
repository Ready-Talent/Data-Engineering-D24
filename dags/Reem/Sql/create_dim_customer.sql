CREATE TABLE IF NOT EXISTS Data_Platform_Reema.dim_customer (
    customer_key INTEGER,
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
    created_by STRING ,
    created_at TIMESTAMP ,
    modified_by STRING,
    modified_at TIMESTAMP
);