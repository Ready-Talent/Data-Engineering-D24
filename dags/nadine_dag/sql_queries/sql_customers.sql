select c.customer_id , c."name" customer_name, c.email , c.phone
, a.address_id , a.street address_street, a.zipcode address_zipcode
, -1 city_id, a.city city_name
, -1 state_id, a.state state_name
, -1 country_id, 'US' country_name
from src01.customer c
left join src01.address a
on a.customer_id =c.customer_id;

