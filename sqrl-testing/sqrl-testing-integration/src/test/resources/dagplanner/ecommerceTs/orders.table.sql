CREATE TABLE Orders (
  `id` BIGINT NOT NULL,
  `customerid` BIGINT NOT NULL,
  `time` TIMESTAMP_LTZ(3) NOT NULL,
  `entries` ROW(`productid` BIGINT NOT NULL, `quantity` BIGINT NOT NULL, `unit_price` DOUBLE NOT NULL, `discount` DOUBLE) NOT NULL ARRAY NOT NULL,
  PRIMARY KEY (`id`, `time`) NOT ENFORCED,
  WATERMARK FOR `time` AS `time` - INTERVAL '0.001' SECOND
) WITH (
  'connector' = 'filesystem',
  'format' = 'flexible-json',
  'path' = 'file:/mock',
  'source.monitor-interval' = '10 sec'
  );
