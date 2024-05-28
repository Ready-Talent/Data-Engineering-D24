WITH fct_trip AS (
    SELECT
        unique_key,
        tr.taxi_id,
        trip_start_timestamp,
        trip_end_timestamp,
        trip_seconds,
        trip_miles,
        fare,
        tips,
        tolls,
        extras,
        trip_total,
        pt.payment_typ_id,
        pl.location_id as pickup_location_id,
        dl.location_id as dropoff_location_id,
        dim_date.date AS trip_date,
        dim_date.day_name AS trip_day_name,
        dim_date.day_of_week AS day_of_week,
        dim_date.day_of_month AS day_of_month,
    FROM 
        `ready-data-engineering-p24.chicago_taxi_OT.chicago-taxi-test-de24_OT` tr
    left join 
        {{ ref('dim_location') }} pl on
        pickup_location = pl.location
        and pickup_latitude = pl.latitude
        and pickup_longitude = pl.longitude
        and pickup_census_tract = pl.census_tract
        and pickup_community_area = pl.community_area
    left join 
        {{ ref('dim_location') }} dl on
        dropoff_location = dl.location
        and dropoff_latitude = dl.latitude
        and dropoff_longitude = dl.longitude
        and dropoff_census_tract = dl.census_tract
        and dropoff_community_area = dl.community_area
    left join 
         {{ ref('dim_payment_type') }} pt on
         pt.payment_type = tr.payment_type
    left join
         {{ ref('dim_taxi') }} t on
         t.taxi_id = tr.taxi_id
    LEFT JOIN
        {{ ref('dim_date') }} dd  ON CAST(FORMAT_DATE('%Y%m%d', DATE(trip_end_timestamp)) AS INT64) = dd.date_id
)
SELECT * FROM fct_trip