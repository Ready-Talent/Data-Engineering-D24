CREATE TABLE IF NOT EXISTS Data_Platform_Rowym.dim_customer (
	customer_id INT64,
	customer_name STRING,
	address_id INT64,
	address_street STRING,
	zipcode INT64,
	created_by STRING,
	created_at TIMESTAMP,
	modified_by STRING,
	modified_at TIMESTAMP
);
