INSERT INTO data_platform_abdelrahman_tamer.fact_sales (customer_key, product_key, date_key, time_key, junk_key, quantity, price, amount, paid_amount)

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
from SRC_06.order_detail od

join SRC_06.order o
on o.order_id = od.order_id

left join SRC_06.payment p
on od.order_detail_id = p.order_detail_id

left join SRC_06.address a
on a.address_id = o.address_id

left join data_platform_abdelrahman_tamer.junk_dim jd
on jd.payment_type_code = coalesce(p.payment_type_id,-1)
and jd.channel_code = o.channel_id

left join data_platform_abdelrahman_tamer.dim_time dt
on dt.full_time = o.order_date :: timestamp :: time

left join data_platform_abdelrahman_tamer.dim_date dd
on dd.date = o.order_date

left join data_platform_abdelrahman_tamer.dim_product dp
on dp.product_id = od.product_id

left join data_platform_abdelrahman_tamer.dim_customer dc
on dc.customer_id = a.customer_id
and dc.address_id = a.address_id;