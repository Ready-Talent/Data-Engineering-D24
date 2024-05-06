CREATE TABLE IF NOT EXISTS `ready-data-engineering-p24.Data_Platform_Abduallah.Fact_sales` (
    customer_key INT64 references `ready-data-engineering-p24.Data_Platform_Abduallah.dim_customer`(customer_key),
    product_key INT64 references `ready-data-engineering-p24.Data_Platform_Abduallah.dim_product`(product_key),
    date_key INT64 references `ready-data-engineering-p24.Data_Platform_Abduallah.dim_date`(date_key),
    time_key INT64 references `ready-data-engineering-p24.Data_Platform_Abduallah.dim_time`(time_key),
    junk_key INT64 references `ready-data-engineering-p24.Data_Platform_Abduallah.junk_dim`(junk_key),
    quantity INT64,
    price FLOAT64,
    amount FLOAT64,
    paid_amount FLOAT64,

    created_by STRING DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    modified_by STRING,
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
 INSERT INTO `ready-data-engineering-p24.Data_Platform_Abduallah.Fact_sales` (customer_key,product_key,date_key,time_key,junk_key,quantity,price,amount,paid_amount,created_by,created_at,modified_by,modified_at)
             SELECT 
            customer_key,product_key,date_key,time_key,junk_key,quantity,price,amount,paid_amount,created_by,created_at,modified_by,modified_at
            FROM
                `ready-data-engineering-p24.data_platform.Fact_sales`