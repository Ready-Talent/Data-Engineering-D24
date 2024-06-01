{{
    config(
        materialized='view'
    )
}}

SELECT 

payment_type

FROM `ready-data-engineering-p24.dbt_othabet.fct_trip` 
