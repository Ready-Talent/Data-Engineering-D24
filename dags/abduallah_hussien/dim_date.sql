-- create dim_date
create table if not exists `ready-data-engineering-p24.Data_Platform_Abduallah.dim_date` (
    date_key INT64,
    date date,
    day_of_week INT64,
    day_name STRING(10),
    day_of_month INT64,
    day_of_year INT64,
    week_of_year INT64,
    month_name STRING(10),
    month_of_year INT64,
    quarter INT64,
    year INT64,
    holiday_flag STRING,
    weekend_flag STRING,
    record_updated_date timestamp default CURRENT_TIMESTAMP()
);
CREATE TABLE IF NOT EXISTS `ready-data-engineering-p24.Data_Platform_Abduallah.dim_product` (
  product_key INT64,
  product_id INT64,
  brand_id INT64,
  category_id INT64,
  name STRING,
  price NUMERIC(10, 2),
  description STRING,
  brand_name STRING,
  category_name STRING,
  created_by STRING DEFAULT 'user',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  modified_by STRING,
  modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)