{{
    config(
        materialized='view'
    )
}}

SELECT 

    t.taxi_id,
    f.tips

 FROM `ready-data-engineering-p24.dbt_othabet.dim_taxi` t
 left join `ready-data-engineering-p24.dbt_othabet.fct_trip` f
 on t.taxi_id = f.taxi_id
 Order by tips desc
 Limit 10