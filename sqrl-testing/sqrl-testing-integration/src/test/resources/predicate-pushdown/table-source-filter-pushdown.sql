CREATE CATALOG ctl WITH (
    'type'='iceberg',
    'catalog-type'='hadoop',
    'warehouse'='file://__tempdir__'
);

USE CATALOG ctl;

CREATE DATABASE IF NOT EXISTS my_database;
USE my_database;

DROP TABLE IF EXISTS items;
CREATE TABLE items (
    id INT,
    category STRING,
    price INT
) PARTITIONED BY (category);

INSERT INTO items VALUES (1,'book',10), (2,'book',30), (3,'toy',20);

SELECT * FROM items /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/ WHERE category = 'book' AND price < 20;
