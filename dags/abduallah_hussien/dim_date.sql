-- create dim_date
create table if not exists `ready-data-engineering-p24.Data_Platform_Abduallah.dim_date` (
    date_key serial primary key,
    date date,
    day_of_week int,
    day_name varchar(10),
    day_of_month int,
    day_of_year int,
    week_of_year int,
    month_name varchar(10),
    month_of_year int,
    quarter int,
    year int,
    holiday_flag boolean,
    weekend_flag boolean,
    record_updated_date timestamp default now()
);