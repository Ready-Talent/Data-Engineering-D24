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