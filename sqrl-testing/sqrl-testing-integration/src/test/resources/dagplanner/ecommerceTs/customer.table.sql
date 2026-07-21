CREATE TABLE Customer (
  `customerid` BIGINT NOT NULL,
  `email` STRING NOT NULL,
  `name` STRING NOT NULL,
  `lastUpdated` BIGINT NOT NULL,
  `timestamp` AS COALESCE(TO_TIMESTAMP_LTZ(lastUpdated, 0), TIMESTAMP '1970-01-01 00:00:00.000'),
  PRIMARY KEY (`customerid`, `lastUpdated`) NOT ENFORCED,
  WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '0.001' SECOND
) WITH (
  'connector' = 'filesystem',
  'format' = 'flexible-json',
  'path' = 'file:/mock',
  'source.monitor-interval' = '10 sec'
);
