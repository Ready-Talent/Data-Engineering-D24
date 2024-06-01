select taxi_id,
tips,
trip_total,
trip_miles,
DATEDIFF(MINUTE,trip_end_timestamp,trip_start_timestamp),
payment_typ_id
FROM
fct_trip