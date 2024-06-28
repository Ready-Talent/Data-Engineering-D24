SELECT 
    unique_key,
    trip_start_timestamp,
    trip_end_timestamp,
    d.day_name,
    d.month,
    d.year,
    d.day_of_week,
    d.day_of_month,
    extras,
    tolls,
    tips,
    fare

 FROM `ready-data-engineering-p24.Nadine_Airflow.chicago-taxi` c
 left join `ready-data-engineering-p24.dbt_nnoureldin.dim_date` d
 on CAST(FORMAT_DATE('%Y%m%d', DATE(c.trip_end_timestamp)) AS INT64) = d.date_id 