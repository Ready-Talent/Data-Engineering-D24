INSERT INTO data_platform_abdelrahman_tamer.dim_junk (payment_type_code, payment_type_name, channel_code, channel_name)
with payment_types as
(
select pt.payment_type_id payment_type_code, pt.name payment_type_name from SRC_06.payment_type pt
union all
select -1 payment_type_code, 'N/A' payment_type_name

)
select row_number() over() as junk_id, pt.payment_type_code , pt.payment_type_name, c.channel_id channel_code, c.name channel_name
from SRC_06.channel c, payment_types pt;
