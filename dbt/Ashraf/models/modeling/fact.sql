WITH fact AS (
    SELECT
        unique_key,
        taxi_id,
        company,
        date_id,
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

    FROM 
        `ready-data-engineering-p24.chicago_taxi_01.chicago-taxi-test-de24` trips
    LEFT JOIN
        {{ ref('dim_date') }}  ON  CAST(FORMAT_DATE('%d', DATE(trip_end_timestamp)) AS INT64) = dim_date.day_of_month
)
SELECT * FROM fact