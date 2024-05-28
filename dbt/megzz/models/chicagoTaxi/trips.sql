
SELECT
    t.unique_key,
    t.taxi_id,
    t.trip_start_timestamp,
    t.trip_end_timestamp,
    t.trip_seconds,
    t.trip_miles,
    pickup.census_tract AS pickup_census_tract,
    pickup.community_area AS pickup_community_area,
    pickup.latitude AS pickup_latitude,
    pickup.longitude AS pickup_longitude,
    pickup.location AS pickup_location,
    dropoff.census_tract AS dropoff_census_tract,
    dropoff.community_area AS dropoff_community_area,
    dropoff.latitude AS dropoff_latitude,
    dropoff.longitude AS dropoff_longitude,
    dropoff.location AS dropoff_location,
    t.fare,
    t.tips,
    t.tolls,
    t.extras,
    t.trip_total,
    payment.payment_method,
    taxi.company,
    date_info.date_id,
    date_info.year,
    date_info.month,
    date_info.day,
    date_info.day_of_week
FROM
    `ready-data-engineering-p24.SRC_08.trips` AS t
LEFT JOIN
    {{ ref('location') }} AS pickup
ON
    t.pickup_census_tract = pickup.census_tract
LEFT JOIN
    {{ ref('location') }} AS dropoff
ON
    t.dropoff_census_tract = dropoff.census_tract
LEFT JOIN
    {{ ref('payment') }} AS payment
ON
    t.payment_type = payment.payment_method
LEFT JOIN
    {{ ref('taxi') }} AS taxi
ON
    t.taxi_id = company.taxi_id
LEFT JOIN
    {{ ref('time') }} AS date_info
ON
    DATE(t.trip_start_timestamp) = date_info.date_id
