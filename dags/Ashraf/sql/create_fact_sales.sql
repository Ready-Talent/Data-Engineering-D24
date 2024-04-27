CREATE TABLE IF NOT EXISTS data_platform_01.Fact_sales (
    customer_key INT64,
    product_key INT64,
    date_key INT64,
    time_key INT64,
    junk_key INT64,

    quantity INT64,
    price Float64,
    amount Float64,
    paid_amount Float64,


);
