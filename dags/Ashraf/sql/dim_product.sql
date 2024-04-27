INSERT INTO data_platform_01.dim_product
(product_key, product_id, brand_id, category_id, name, price, description, brand_name, category_name)

select  ROW_NUMBER() OVER () + COALESCE((SELECT MAX(product_key) FROM data_platform_01.dim_product), 0)
, p.product_id , -1 brand_id, -1 category_id
,REGEXP_EXTRACT(name, r'\s(.*)') AS product_name
,p.price
,p.description
,  SPLIT(SPLIT(p.name, ' ')[OFFSET(0)], ' ')[OFFSET(0)] AS brand_name
, 'Cell Phones' category_name
from landing.product p;
