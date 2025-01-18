CREATE TABLE Customer (
  `timestamp` AS TO_TIMESTAMP_LTZ(lastUpdated, 0),
  PRIMARY KEY (customerid, lastUpdated) NOT ENFORCED,
  WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '0.001' SECOND
) WITH (
  'format' = 'flexible-json',
  'path' = 'file:/mock',
  'source.monitor-interval' = '10 sec',
  'connector' = 'filesystem'
);