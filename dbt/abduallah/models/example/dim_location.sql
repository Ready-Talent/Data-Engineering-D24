WITH dim_pickup_location AS (
    SELECT DISTINCT
        pickup_location,
        pickup_latitude,
        pickup_longitude,
        pickup_census_tract,
        pickup_community_area,
        --ROW_NUMBER() OVER (PARTITION BY pickup_location ORDER BY pickup_census_tract DESC) AS pickup_location_id
    FROM `ready-data-engineering-p24.chicago_taxi_OT.chicago-taxi-test-de24_OT`
    WHERE
        pickup_location IS NOT NULL
),

dim_dropoff_location AS (
    SELECT DISTINCT
        dropoff_location,
        dropoff_latitude,
        dropoff_longitude,
        dropoff_census_tract,
        dropoff_community_area,
        --ROW_NUMBER() OVER (PARTITION BY dropoff_location ORDER BY dropoff_census_tract DESC) AS dropoff_location_id
    FROM `ready-data-engineering-p24.chicago_taxi_OT.chicago-taxi-test-de24_OT`
    WHERE
        dropoff_location IS NOT NULL
),

location AS (
    SELECT DISTINCT
        Dense_rank() OVER (ORDER BY pickup_census_tract DESC) AS location_id,
        pickup_location AS location,
        pickup_latitude AS latitude,
        pickup_longitude AS longitude,
        pickup_census_tract AS census_tract,
        pickup_community_area AS community_area
    FROM 
        dim_dropoff_location d
    join dim_pickup_location p
    on dropoff_location = pickup_location
    and dropoff_latitude = pickup_latitude
    and dropoff_longitude = pickup_longitude
    and dropoff_census_tract = pickup_census_tract
    and dropoff_community_area = pickup_community_area
)

SELECT
    *
FROM
    location