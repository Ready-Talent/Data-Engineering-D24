WITH dim_payment_type AS (
    SELECT DISTINCT
        Dense_rank() OVER (order by payment_type) AS payment_typ_id,
        payment_type
    FROM `ready-data-engineering-p24.chicago_taxi_OT.chicago-taxi-test-de24_OT`
)
SELECT * FROM dim_payment_type