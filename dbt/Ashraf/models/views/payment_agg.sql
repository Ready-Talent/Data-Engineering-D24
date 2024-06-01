
with cte as (
     select
        payment_type,
       count(payment_type) as payment_count
    from dbt_mashraf.fact
    group by payment_type
)

select * from cte
