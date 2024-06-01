
with cte as (
     select
       count(payment_type)
    from dbt_mashraf.fact
    group by payment_type
)

select * from cte
