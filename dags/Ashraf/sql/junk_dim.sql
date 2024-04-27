INSERT INTO data_platform_01.junk_dim (payment_type_code, payment_type_name, channel_code, channel_name)
WITH payment_types AS (
    SELECT pt.payment_type_id AS payment_type_code, pt.name AS payment_type_name 
    FROM landing.payment_type pt
    UNION ALL
    (SELECT -1 AS payment_type_code, 'N/A' AS payment_type_name)
)
SELECT pt.payment_type_code, pt.payment_type_name, c.channel_id AS channel_code, c.name AS channel_name
FROM landing.channel c, payment_types pt;
