create table if not exists `ready-data-engineering-p24.Data_Platform_Abduallah.dim_date` (
    date_key INT64,
    date date,
    day_of_week INT64,
    day_name STRING(10),
    day_of_month INT64,
    day_of_year INT64,
    week_of_year INT64,
    month_name STRING(10),
    month_of_year INT64,
    quarter INT64,
    year INT64,
    --holiday_flag STRING,
    --weekend_flag STRING,
    record_updated_date timestamp default CURRENT_TIMESTAMP()
);
delete from `ready-data-engineering-p24.Data_Platform_Abduallah.dim_date` where cast (date_key as int) <= 5000;
 INSERT INTO `ready-data-engineering-p24.Data_Platform_Abduallah.dim_date` (date_key,date,day_of_week,day_name,day_of_month,day_of_year,week_of_year,month_name,month_of_year,quarter,year)
             SELECT 
            date_key,date,day_of_week,day_name,day_of_month,day_of_year,week_of_year,month_name,month_of_year,quarter,year
            FROM
                `ready-data-engineering-p24.data_platform.dim_date`