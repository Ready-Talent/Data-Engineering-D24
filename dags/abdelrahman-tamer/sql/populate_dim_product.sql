INSERT INTO data_platform_abdelrahman_tamer.product
(product_id, brand_id, category_id, product_name, price, description, brand_name, category_name)

SELECT
  p.product_id,
  -1 brand_id,
  -1 category_id,
  p.name AS product_name,
  p.price,
  p.description,
  SPLIT(p.name, ' ')[0] AS brand_name,
  'Cell Phones' category_name
FROM
  SRC_06.product p;