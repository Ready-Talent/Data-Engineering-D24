INSERT INTO data_platform_01.dim_customer
(customer_key, customer_id, customer_name, email, phone , address_id, address_street, address_zipcode, city_id, city_name
, state_id, state_name, country_id, country_name)

select  ROW_NUMBER() OVER () + COALESCE((SELECT MAX(customer_key) FROM data_platform_01.dim_customer), 0), c.customer_id , c.name customer_name, c.email , c.phone
, a.address_id , a.street address_street, a.zipcode address_zipcode
, -1 city_id, a.city city_name
, -1 state_id, a.state state_name
, -1 country_id, 'US' country_name
from landing.customer c
left join landing.address a
on a.customer_id=c.customer_id;