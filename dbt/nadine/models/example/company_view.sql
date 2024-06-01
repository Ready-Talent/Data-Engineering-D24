{{
    config(
        materialized='view'
    )
}}

SELECT 

company,
trip_miles,
trip_seconds,
trip_total,

FROM `ready-data-engineering-p24.dbt_othabet.fct_trip` l
Limit 10