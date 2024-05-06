CREATE TABLE IF NOT EXISTS PL.junk_dim (
    junk_key INT64,
    payment_type_code INT64,
    payment_type_name STRING,
    channel_code INT64,
    channel_name STRING,
    created_by STRING DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    modified_by STRING,
    modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
 INSERT INTO `ready-data-engineering-p24.Data_Platform_Abduallah.junk_dim` (junk_key,payment_type_code,payment_type_name,channel_code,channel_name,created_by,created_at,modified_by,modified_at)
             SELECT 
            junk_key,payment_type_code,payment_type_name,channel_code,channel_name,created_by,created_at,modified_by,modified_at
            FROM
                `ready-data-engineering-p24.data_platform.junk_dim`