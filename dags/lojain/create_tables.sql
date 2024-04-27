CREATE TABLE IF NOT EXISTS Data_Platform_Lojain.dim_customer (
    customer_key INT64,
    customer_id INT64,
    customer_name STRING,
    email STRING,
    phone STRING,
    address_id INT64,
    address_street STRING,
    address_zipcode STRING,
    city_id INT64,
    city_name STRING,
    state_id INT64,
    state_name STRING,
    country_id INT64,
    country_name STRING,
--     contact_id INTEGER,
--     contact_type_code STRING,
--     contact_info STRING,
    created_by STRING DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    modified_by STRING,
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
-- create dim_date
CREATE TABLE IF NOT EXISTS Data_Platform_Lojain.dim_product (
    product_key INT64,
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


CREATE TABLE IF NOT EXISTS Data_Platform_Lojain.dim_date (
    date_key INT64,
    date DATE,
    day_of_week INT64,
    day_name STRING,
    day_of_month INT64,
    day_of_year INT64,
    week_of_year INT64,
    month_name STRING,
    month_of_year INT64,
    quarter INT64,
    year INT64,
    holiday_flag BOOL,
    weekend_flag BOOL,
    record_updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS Data_Platform_Lojain.dim_time (
    time_key INT64,
    full_time TIME,
    hour INT64,
    minute INT64
);
CREATE TABLE IF NOT EXISTS Data_Platform_Lojain.junk_dim (
    junk_key INT64,
    payment_type_code INT64,
    payment_type_name STRING,
    channel_code INT64,
    channel_name STRING,
    created_by STRING DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    modified_by STRING,
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
