WITH dim_taxi AS (
    SELECT DISTINCT
        taxi_id,
        company
    FROM `ready-data-engineering-p24.chicago_taxi_OT.chicago-taxi-test-de24_OT`
`
)
SELECT * FROM dim_taxi