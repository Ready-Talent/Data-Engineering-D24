SELECT
        pickup_census_tract AS census_tract,
        pickup_community_area AS community_area,
        pickup_latitude AS latitude,
        pickup_longitude AS longitude,
        pickup_location AS location
    FROM `ready-data-engineering-p24.SRC_08.trips`
    UNION
    SELECT
        dropoff_census_tract AS census_tract,
        dropoff_community_area AS community_area,
        dropoff_latitude AS latitude,
        dropoff_longitude AS longitude,
        dropoff_location AS location
    FROM `ready-data-engineering-p24.SRC_08.trips`
