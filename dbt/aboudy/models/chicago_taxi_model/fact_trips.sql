{{
    config(
        materialized='incremental',
        unique_key='trip_id'
    )
}}

select 
    unique_key as trip_id,
    trips.taxi_id,
    dim_taxi.company as taxi_company_name,
    trip_start_timestamp,
    trip_end_timestamp,
    trip_seconds,
    trip_miles,
    pickup_census_tract,
    dropoff_census_tract,
    pickup_community_area,
    dropoff_community_area,
    fare,
    tips,
    tolls,
    extras,
    trip_total,
    pickup_latitude,
    pickup_longitude,
    pickup_location,
    dropoff_latitude,
    dropoff_longitude,
    dropoff_location
from `ready-data-engineering-p24.SRC_06.chicago_taxi` trips
left join {{ref('dim_taxi')}} on trips.taxi_id = dim_taxi.taxi_id