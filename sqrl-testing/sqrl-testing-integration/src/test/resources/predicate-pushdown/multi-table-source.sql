CREATE TABLE fs_t (
    a INT,
    country STRING,
    v INT,
    dt STRING
) PARTITIONED BY (dt)
WITH (
    'connector' = 'filesystem',
    'path' = 'file://__tempdir__',
    'format' = 'csv',
    'source.monitor-interval' = '1s'
);

CREATE VIEW dummy_view AS
    SELECT CONCAT(country, '-x') AS my_country, country, v + 5 AS z, dt, a FROM fs_t;

SELECT my_country AS country, z FROM dummy_view
UNION ALL
SELECT dt AS country, a AS z FROM dummy_view;
