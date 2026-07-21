CREATE TABLE Product (
  `productid` BIGINT NOT NULL,
  `name` STRING NOT NULL,
  `description` STRING NOT NULL,
  `category` STRING NOT NULL,
  `_ingest_time` TIMESTAMP_LTZ(3) NOT NULL,
  PRIMARY KEY (`productid`, `name`, `description`, `category`) NOT ENFORCED,
  WATERMARK FOR `_ingest_time` AS `_ingest_time` - INTERVAL '0.001' SECOND
) WITH (
  'connector' = 'filesystem',
  'format' = 'flexible-json',
  'path' = 'file:/mock',
  'source.monitor-interval' = '10 sec'
  );
