{{
    config(
        materialized='table'
    )
}}

SELECT
  date,
  EXTRACT(YEAR FROM date) AS year,
  EXTRACT(QUARTER FROM date) AS quarter,
  EXTRACT(MONTH FROM date) AS month,
  EXTRACT(DAY FROM date) AS day,
  EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
  FORMAT_DATE('%A', date) AS day_name,
  CASE WHEN EXTRACT(DAYOFWEEK FROM date) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend,
  CASE WHEN date IN ('2024-01-01', '2024-12-25') THEN TRUE ELSE FALSE END AS is_holiday
FROM
  UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2025-12-31')) AS date