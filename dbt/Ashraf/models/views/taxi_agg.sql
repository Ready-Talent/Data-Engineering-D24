
with cte as (
     select
      sum(tips) as total_tips,
    from dbt_mashraf.fact
    group by taxi_id
)

select * from cte
