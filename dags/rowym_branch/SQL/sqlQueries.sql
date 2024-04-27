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

INSERT INTO Data_Platform_Rowym.dim_customer (customer_id, customer_name, address_id, address_street, zipcode, created_by, created_at, modified_by, modified_at)
select 
	customer.customer_id as customer_id,
	customer.name as customer_name,
	address.address_id as address_id,
	address.street as address_street,
	address.zipcode as address_zipcode,
	customer.created_by as created_by,
	customer.created_at as created_at,
	customer.modified_by as modified_by,
	customer.modified_at as modified_at 
from SRC_05.customer  
left join SRC_05.address on address.customer_id = customer.customer_id ;