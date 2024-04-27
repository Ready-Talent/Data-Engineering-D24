INSERT INTO data_platform_01.dim_product
(product_id, brand_id, category_id, name, price, description, brand_name, category_name)

select p.product_id , -1 brand_id, -1 category_id
,REGEXP_EXTRACT(name, r'\s(.*)') AS product_name
,p.price
,p.description
, split_part(p.name, ' ', 1) AS brand_name
, 'Cell Phones' category_name
from landing.product p;