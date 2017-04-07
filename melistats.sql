--CREATE DATABASE  meli_stats OWNER mf ENCODING 'UTF8'; 


CREATE TABLE categories_stats(
    category_id varchar PRIMARY KEY,
    stats_date date,
    name text,
    units_sold integer,
    income float,
    visits integer
);

