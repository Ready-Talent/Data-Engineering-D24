WITH fct_trip AS (
    SELECT
        unique_key,
        taxi_id,
        company,
        trip_start_timestamp,
        trip_end_timestamp,
        trip_seconds,
        trip_miles,
        fare,
        tips,
        tolls,
        extras,
        trip_total,
        payment_type,
        pickup_location,
        pickup_latitude,
        pickup_longitude,
        pickup_census_tract,
        pickup_community_area,
        pickup_location
        dropoff_location,
        dropoff_latitude,
        dropoff_longitude,
        dropoff_census_tract,
        dropoff_community_area,
        dim_date.date AS trip_date,
        dim_date.day_name AS trip_day_name,
        dim_date.day_of_week AS day_of_week,
        dim_date.day_of_month AS day_of_month,
        dim_date.is_weekend AS is_weekend,
        dim_date.public_holiday_name AS public_holiday_name
    FROM 
        `ready-data-engineering-p24.chicago_taxi_OT.chicago-taxi-test-de24_OT` trips
    LEFT JOIN
        {{ ref('dim_date') }}  ON CAST(FORMAT_DATE('%Y%m%d', DATE(trips.trip_end_timestamp)) AS INT64) = dim_date.date_id
)
SELECT * FROM fct_trip