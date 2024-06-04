{{
    config(
        materialized='view',
    )
}}


select
    taxi_id,
    SUM(tips) as total_tips,
from `ready-data-engineering-p24.dbt_othabet.fct_trip`
group by 1

