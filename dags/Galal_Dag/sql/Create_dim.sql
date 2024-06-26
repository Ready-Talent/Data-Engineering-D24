
-- Drop existing tables if they exist
DROP TABLE IF EXISTS Data_Platform_Galal.Fact_sales;
DROP TABLE IF EXISTS Data_Platform_Galal.dim_date;
DROP TABLE IF EXISTS Data_Platform_Galal.dim_time;
DROP TABLE IF EXISTS Data_Platform_Galal.dim_customer;
DROP TABLE IF EXISTS Data_Platform_Galal.dim_product;
DROP TABLE IF EXISTS Data_Platform_Galal.junk_dim;

-- Create dimension tables
-- dim_date
CREATE TABLE if not exists  Data_Platform_Galal.dim_customer (
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    modified_by STRING,
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- dim_time
create table if not exists Data_Platform_Galal.dim_date (
    date_key INT64,
    date date,
    day_of_week INT64,
    day_name INT64,
    day_of_month INT64,
    day_of_year INT64,
    week_of_year INT64,
    month_name STRING,
    month_of_year INT64,
    quarter INT64,
    year INT64,
);

-- dim_customer
CREATE TABLE IF NOT EXISTS Data_Platform_Galal.dim_product (
    product_key INT64,
    product_id INT64,
    brand_id INT64,
    category_id INT64,
    name STRING,
    price INT64,
    description STRING,
    brand_name STRING,
    category_name STRING,
    created_by STRING DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    modified_by STRING,
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- dim_product
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


);

CREATE TABLE IF NOT EXISTS Data_Platform_Galal.junk_dim (
    junk_key INT64,
    payment_type_code INT64,
    payment_type_name STRING,
    channel_code INT64,
    channel_name STRING,
);


