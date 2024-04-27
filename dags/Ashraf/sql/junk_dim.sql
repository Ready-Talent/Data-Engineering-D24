INSERT INTO data_platform_01.junk_dim
(payment_type_code, payment_type_name, channel_code, channel_name)
with payment_types as
(
select pt.payment_type_id payment_type_code, pt."name" payment_type_name from landing.payment_type pt
union
select -1 payment_type_code, 'N/A' payment_type_name

)
select pt.payment_type_code , pt.payment_type_name, c.channel_id channel_code, c."name" channel_name
from landing.channel c, payment_types pt;
