CREATE TABLE Product (
  `productid` BIGINT NOT NULL,
  `name` STRING NOT NULL,
  `description` STRING NOT NULL,
  `category` STRING NOT NULL,
  `_ingest_time` AS PROCTIME(),
  PRIMARY KEY (`productid`, `name`, `description`, `category`) NOT ENFORCED
) WITH (
    'connector' = 'filesystem',
    'format' = 'json',
    'path' = 'file:/mock',
    'source.monitor-interval' = '10 sec'
);
