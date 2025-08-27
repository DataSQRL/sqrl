CREATE TABLE Orders (
  PRIMARY KEY (id, `time`) NOT ENFORCED,
  WATERMARK FOR `time` AS `time` - INTERVAL '0.001' SECOND
) WITH (
  'format' = 'flexible-json',
  'path' = 'file:/mock',
  'source.monitor-interval' = '10 sec',
  'connector' = 'filesystem'
  )
LIKE `Orders.schema.yml`;
