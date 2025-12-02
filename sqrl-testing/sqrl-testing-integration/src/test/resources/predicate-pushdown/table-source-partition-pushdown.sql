CREATE TABLE fs_t (
    a INT,
    country STRING,
    v INT,
    dt STRING
) PARTITIONED BY (dt)
WITH (
    'connector' = 'filesystem',
    'path' = 'file://__tempdir__',
    'format' = 'csv'
);

INSERT INTO fs_t PARTITION (dt='2025-01-01') VALUES (1, 'DE', 5);
INSERT INTO fs_t PARTITION (dt='2025-01-02') VALUES (2, 'US', 50);

SELECT a, country FROM fs_t WHERE dt = '2025-01-01' AND v > 10;
