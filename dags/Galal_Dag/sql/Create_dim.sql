
-- Drop existing tables if they exist
DROP TABLE IF EXISTS Data_Platform_Galal.Fact_sales;
DROP TABLE IF EXISTS Data_Platform_Galal.dim_date;
DROP TABLE IF EXISTS Data_Platform_Galal.dim_time;
DROP TABLE IF EXISTS Data_Platform_Galal.dim_customer;
DROP TABLE IF EXISTS Data_Platform_Galal.dim_product;
DROP TABLE IF EXISTS Data_Platform_Galal.junk_dim;

-- Create dimension tables

-- dim_date
CREATE TABLE IF NOT EXISTS Data_Platform_Galal.dim_date (
    date_key INT64 ,
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

-- dim_time
CREATE TABLE IF NOT EXISTS Data_Platform_Galal.dim_time (
    time_key INT64 ,
    full_time TIME,
    hour INT64,
    minute INT64
);

-- dim_customer
CREATE TABLE IF NOT EXISTS Data_Platform_Galal.dim_customer (
    customer_key INT64 ,
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
    created_by STRING DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    modified_by STRING,
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- dim_product
CREATE TABLE IF NOT EXISTS Data_Platform_Galal.dim_product (
    product_key INT64 ,
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

-- junk_dim
CREATE TABLE IF NOT EXISTS Data_Platform_Galal.junk_dim (
    junk_key INT64 ,
    payment_type_code INT64,
    payment_type_name STRING,
    channel_code INT64,
    channel_name STRING,
    created_by STRING DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    modified_by STRING,
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Fact_sales
CREATE TABLE IF NOT EXISTS Data_Platform_Galal.Fact_sales (
    customer_key INT64,
    product_key INT64,
    date_key INT64,
    time_key INT64,
    junk_key INT64,
    quantity INT64,
    price NUMERIC(10, 2),
    amount NUMERIC(10, 2),
    paid_amount NUMERIC(10, 2),
    created_by STRING DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    modified_by STRING,
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
