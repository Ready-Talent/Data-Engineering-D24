WITH dim_location_ranked AS (
    SELECT
        pickup_location AS location,
        pickup_latitude AS latitude,
        pickup_longitude AS longitude,
        pickup_census_tract AS census_tract,
        pickup_community_area AS community_area,
        ROW_NUMBER() OVER (PARTITION BY pickup_location ORDER BY pickup_census_tract DESC) AS row_num
    FROM `ready-data-engineering-p24.chicago_taxi_OT.chicago-taxi-test-de24_OT`
    WHERE
        pickup_location IS NOT NULL
)
SELECT
    location,
    latitude,
    longitude,
    census_tract,
    community_area
FROM
    dim_location_ranked
WHERE
    row_num = 1