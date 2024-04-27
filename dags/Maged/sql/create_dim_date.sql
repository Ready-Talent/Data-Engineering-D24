create table if not exists Data_Platform_Maged.dim_date (
    date_key INTEGER,
    date date,
    day_of_week INTEGER,
    day_name INTEGER,
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    month_name STRING,
    month_of_year INTEGER,
    quarter INTEGER,
    year INTEGER,
);