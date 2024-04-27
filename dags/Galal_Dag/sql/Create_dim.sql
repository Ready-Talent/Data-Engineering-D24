CREATE SCHEMA if not exists Data_Platform_Galal;

drop table if exists Data_Platform_Galal.Fact_sales;
drop table if exists Data_Platform_Galal.dim_date;
drop table if exists Data_Platform_Galal.dim_time;
drop table if exists Data_Platform_Galal.dim_customer;
drop table if exists Data_Platform_Galal.dim_product;
drop table if exists Data_Platform_Galal.junk_dim;

-- dimension tables:

-- create dim_date
create table if not exists Data_Platform_Galal.dim_date (
    date_key serial primary key,
    date date,
    day_of_week int,
    day_name varchar(10),
    day_of_month int,
    day_of_year int,
    week_of_year int,
    month_name varchar(10),
    month_of_year int,
    quarter int,
    year int,
    holiday_flag boolean,
    weekend_flag boolean,
    record_updated_date timestamp default now()
);

CREATE TABLE IF NOT EXISTS Data_Platform_Galal.dim_time (
    time_key serial PRIMARY KEY,
    full_time TIME,
    hour INTEGER,
    minute INTEGER
);

CREATE TABLE IF NOT EXISTS Data_Platform_Galal.dim_customer (
    customer_key serial PRIMARY KEY,
    customer_id INTEGER,
    customer_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(20),
    address_id INTEGER,
    address_street VARCHAR(255),
    address_zipcode VARCHAR(20),
    city_id INTEGER,
    city_name VARCHAR(255),
    state_id INTEGER,
    state_name VARCHAR(255),
    country_id INTEGER,
    country_name VARCHAR(255),
--     contact_id INTEGER,
--     contact_type_code VARCHAR(10),
--     contact_info VARCHAR(255),
    created_by VARCHAR(100) DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100),
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS Data_Platform_Galal.dim_product (
    product_key serial PRIMARY KEY,
    product_id INTEGER,
    brand_id INTEGER,
    category_id INTEGER,
    name VARCHAR(100),
    price DECIMAL(10, 2),
    description TEXT,
    brand_name VARCHAR(100),
    category_name VARCHAR(100),
    created_by VARCHAR(100) DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100),
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS Data_Platform_Galal.junk_dim (
    junk_key serial PRIMARY KEY,
    payment_type_code INTEGER,
    payment_type_name VARCHAR(100),
    channel_code INTEGER,
    channel_name VARCHAR(100),
    created_by VARCHAR(100) DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100),
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS Data_Platform_Galal.Fact_sales (
    customer_key INTEGER references Data_Platform_Galal.dim_customer(customer_key),
    product_key INTEGER references Data_Platform_Galal.dim_product(product_key),
    date_key INTEGER references Data_Platform_Galal.dim_date(date_key),
    time_key INTEGER references Data_Platform_Galal.dim_time(time_key),
    junk_key INTEGER references Data_Platform_Galal.junk_dim(junk_key),

    quantity INTEGER,
    price DECIMAL(10, 2),
    amount DECIMAL(10, 2),
    paid_amount DECIMAL(10, 2),

    created_by VARCHAR(100) DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100),
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);