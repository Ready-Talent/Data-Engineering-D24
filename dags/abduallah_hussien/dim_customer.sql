CREATE TABLE IF NOT EXISTS `ready-data-engineering-p24.Data_Platform_Abduallah.dim_customer` (
    customer_key INT64,
    customer_id INT64,
    customer_name STRING(255),
    email STRING(255),
    phone STRING(20),
    address_id INT64,
    address_street STRING(255),
    address_zipcode STRING(20),
    city_id INT64,
    city_name STRING(255),
    state_id INT64,
    state_name STRING(255),
    country_id INT64,
    country_name STRING(255),
--     contact_id INT64,
--     contact_type_code STRING(10),
--     contact_info STRING(255),
    created_by STRING(100) DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    modified_by STRING(100),
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
 INSERT INTO `ready-data-engineering-p24.Data_Platform_Abduallah.dim_customer` (customer_key,customer_id,customer_name,email,phone,address_id,address_street,address_zipcode,city_id,city_name,state_id,state_name,country_id,country_name)
            SELECT 
            customer_key,customer_id,customer_name,email,phone,address_id,address_street,address_zipcode,city_id,city_name,state_id,state_name,country_id,country_name
            FROM
                `ready-data-engineering-p24.data_platform.dim_customer`