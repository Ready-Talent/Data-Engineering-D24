-- dimension tables:-- create dim_date
create table if not exists Data_Platform_Sedawy.dim_date (
date_key INTEGER,
date date,
day_of_week INT64,
day_name STRING,
day_of_month INT64,
day_of_year INT64,
week_of_year INT64,
month_name STRING,
month_of_year INT64,
quarter INT64,
year INT64,
);

CREATE TABLE IF NOT EXISTS Data_Platform_Sedawy.dim_time (
time_key INT64,
full_time TIME,
hour INT64,
minute INT64
);

CREATE TABLE IF NOT EXISTS Data_Platform_Sedawy.dim_customer (
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

CREATE TABLE IF NOT EXISTS Data_Platform_Sedawy.dim_product (
product_key INT64,
product_id INT64,
brand_id INT64,
category_id INT64,
name STRING,
price INT64,
description STRING,
brand_name STRING,
category_name STRING,
created_by STRING,
created_at TIMESTAMP ,
modified_by STRING,
modified_at TIMESTAMP
);
