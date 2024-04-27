INSERT INTO Data_Platform_Maged.dim_product
(product_id, brand_id, category_id, name, price, description, brand_name, category_name)

select p.product_id , -1 brand_id, -1 category_id
,p.name As product_name
,p.price
,p.description
, split(p.name, ' ')[0] AS brand_name
, 'Cell Phones' category_name
from landing.product p;