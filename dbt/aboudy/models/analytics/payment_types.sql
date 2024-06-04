{{
    config(
        materialized='view'
    )
}}


select
    payment_type,
    count(*) as number_of_trips
from `ready-data-engineering-p24.dbt_othabet.fct_trip`
group by 1