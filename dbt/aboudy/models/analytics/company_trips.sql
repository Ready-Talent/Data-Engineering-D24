{{
    config(
        materialized='view',
    )
}}





select
    trips.company,
    SUM(trips.trip_total - trips.tips) as total_revenue,
    SUM(trips.trip_miles) as total_miles,
    SUM(trips.trip_seconds) as total_seconds
from `ready-data-engineering-p24.dbt_othabet.fct_trip` trips
group by 1

