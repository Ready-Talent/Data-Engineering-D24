
with cte as (
     select
     taxi_id,
      sum(tips) as total_tips,
    from dbt_mashraf.fact
    group by taxi_id
)

select * from cte
