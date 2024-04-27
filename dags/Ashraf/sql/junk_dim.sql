INSERT INTO data_platform_01.junk_dim (junk_key, payment_type_code, payment_type_name, channel_code, channel_name)
WITH payment_types AS (
    SELECT pt.payment_type_id AS payment_type_code, pt.name AS payment_type_name 
    FROM landing.payment_type pt
    UNION ALL
    (SELECT -1 AS payment_type_code, 'N/A' AS payment_type_name)
)
SELECT ROW_NUMBER() OVER () + COALESCE((SELECT MAX(junk_key) FROM data_platform_01.junk_dim), 0)
,pt.payment_type_code
,pt.payment_type_name
,c.channel_id AS channel_code, c.name AS channel_name
FROM landing.channel c, payment_types pt;
