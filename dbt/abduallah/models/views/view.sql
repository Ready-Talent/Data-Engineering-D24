select taxi_id,
tips,
trip_total,
trip_miles,
trip_seconds,
payment_typ_id
FROM
{{ ref('fact_trips') }}