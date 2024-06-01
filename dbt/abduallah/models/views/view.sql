select taxi_id,
tips,
trip_total,
trip_miles,
timestamp_diff(trip_end_timestamp,trip_start_timestamp),
payment_typ_id
FROM
{{ ref('fact_trips') }}