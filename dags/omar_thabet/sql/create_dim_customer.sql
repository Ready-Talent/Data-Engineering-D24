CREATE TABLE  data_platform.dim_customer (
    customer_key INT64,
    customer_id INT64,
    customer_name STRING,
    email STRING,
    phone INT64,
    address_id INT64,
    address_street STRING,
    address_zipcode INT64,
    city_id INT64,
    city_name STRING,
    state_id INT64,
    state_name STRING,
    country_id INT64,
    country_name STRING,
    created_by STRING ,
    created_at TIMESTAMP ,
    modified_by STRING,
    modified_at TIMESTAMP
);