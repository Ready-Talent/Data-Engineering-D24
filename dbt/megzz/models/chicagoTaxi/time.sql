SELECT
    date AS date_id,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(DAY FROM date) AS day,
    FORMAT_DATE('%A', date) AS day_of_week
FROM
    UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2025-12-31', INTERVAL 1 DAY)) AS date