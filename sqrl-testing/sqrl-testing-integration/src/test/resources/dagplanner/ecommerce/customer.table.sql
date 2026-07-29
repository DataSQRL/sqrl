CREATE TABLE Customer (
    customerid BIGINT NOT NULL,
    email STRING,
    name STRING NOT NULL,
    lastUpdated BIGINT NOT NULL,
    `_ingest_time` AS PROCTIME(),
  PRIMARY KEY (customerid, lastUpdated) NOT ENFORCED
) WITH (
    'connector' = 'filesystem',
    'format' = 'json',
    'path' = 'file:/mock',
    'source.monitor-interval' = '10 sec'
);
