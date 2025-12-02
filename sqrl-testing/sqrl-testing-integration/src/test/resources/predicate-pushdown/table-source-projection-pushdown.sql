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

SELECT a, country FROM fs_t;
