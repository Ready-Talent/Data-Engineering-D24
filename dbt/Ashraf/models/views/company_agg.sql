
with cte as (
     select
      sum(trip_total) as money_generated,
      sum(trip_miles) as distance,
      sum(trip_seconds) as trip_time

    from dbt_mashraf.fact
    group by company
)

select * from cte
