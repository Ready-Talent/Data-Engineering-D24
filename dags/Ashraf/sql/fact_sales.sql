INSERT INTO data_platform_01.fact_sales
(customer_key, product_key, date_key, time_key, junk_key, quantity, price, amount, paid_amount)

select
 dc.customer_key
,dp.product_key
,dd.date_key
,dt.time_key
,jd.junk_key
,od.quantity
,od.price
,(od.quantity * od.price ) amount
,coalesce(p.amount,0) paid_amount
from src01.order_detail od

join landing.order o
on o.order_id = od.order_id

left join landing.payment p
on od.order_detail_id = p.order_detail_id

left join landing.address a
on a.address_id = o.address_id

left join data_platform_01.junk_dim jd
on jd.payment_type_code = coalesce(p.payment_type_id,-1)
and jd.channel_code = o.channel_id

left join data_platform.dim_time dt
on CAST(dt.full_time AS TIME) = TIME(o.order_date)

left join data_platform_01.dim_date dd
on dd.date = o.order_date

left join data_platform_01.dim_product dp
on dp.product_id = od.product_id

left join data_platform_01.dim_customer dc
on dc.customer_id = a.customer_id
and dc.address_id = a.address_id;
