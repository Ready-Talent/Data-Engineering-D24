CREATE TABLE IF NOT EXISTS Data_Platform_Reema.dim_customer (
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
    created_by STRING DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    modified_by STRING,
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


CREATE TABLE IF NOT EXISTS Data_Platform_Reema.dim_product (
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


CREATE TABLE IF NOT EXISTS Data_Platform_Reema.dim_date (
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


CREATE TABLE IF NOT EXISTS Data_Platform_Reema.dim_time (
    time_key INT64,
    full_time TIME,
    hour INT64,
    minute INT64
);


CREATE TABLE IF NOT EXISTS Data_Platform_Reema.junk_dim (
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


CREATE TABLE IF NOT EXISTS Data_Platform_Reema.Fact_sales (
    customer_key INTEGER references Data_Platform_Reema.dim_customer(customer_key),
    product_key INTEGER references Data_Platform_Reema.dim_product(product_key),
    date_key INTEGER references Data_Platform_Reema.dim_date(date_key),
    time_key INTEGER references Data_Platform_Reema.dim_time(time_key),
    junk_key INTEGER references Data_Platform_Reema.junk_dim(junk_key),

    quantity INTEGER,
    price DECIMAL(10, 2),
    amount DECIMAL(10, 2),
    paid_amount DECIMAL(10, 2),

    created_by STRING DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_by STRING,
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);