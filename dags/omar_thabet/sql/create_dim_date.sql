create table if not exists data_platform.dim_date (
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
);