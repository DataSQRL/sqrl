CREATE TABLE Customer (
  `_ingest_time` AS PROCTIME(),
  PRIMARY KEY (customerid, lastUpdated) NOT ENFORCED
) WITH (
      'format' = 'json',
      'path' = 'file:/mock',
      'source.monitor-interval' = '10 sec',
      'connector' = 'filesystem'
      );
