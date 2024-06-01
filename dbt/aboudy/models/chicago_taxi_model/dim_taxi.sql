{{
    config(
        materialized='incremental', 
        unique_key='taxi_id'
    )
    
}}

select
    distinct taxi_id,
    company
from `ready-data-engineering-p24.SRC_06.chicago_taxi`