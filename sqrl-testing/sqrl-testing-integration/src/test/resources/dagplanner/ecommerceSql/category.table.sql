CREATE TABLE Category (
  categoryid INT NOT NULL,
  name STRING NOT NULL,
  description STRING,
  `updateTime` AS NOW(),
  WATERMARK FOR `updateTime` AS `updateTime` - INTERVAL '0.001' SECOND
) WITH (
  'format' = 'flexible-json',
  'path' = 'file:/mock',
  'source.monitor-interval' = '10 sec',
  'connector' = 'filesystem'
);