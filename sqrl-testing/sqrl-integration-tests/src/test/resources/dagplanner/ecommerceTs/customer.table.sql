CREATE TABLE Customer (
  `timestamp` AS COALESCE(TO_TIMESTAMP_LTZ(lastUpdated, 0), TIMESTAMP '1970-01-01 00:00:00.000'),
  PRIMARY KEY (customerid, lastUpdated) NOT ENFORCED,
  WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '0.001' SECOND
) WITH (
  'format' = 'flexible-json',
  'path' = 'file:/mock',
  'source.monitor-interval' = '10 sec',
  'connector' = 'filesystem'
);