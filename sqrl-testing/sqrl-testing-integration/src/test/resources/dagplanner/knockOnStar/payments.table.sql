CREATE TABLE Payments (
  `id` BIGINT NOT NULL,
  `amount` DOUBLE NOT NULL,
  `time` TIMESTAMP_LTZ(3) NOT NULL,
  PRIMARY KEY (`id`, `time`) NOT ENFORCED,
  WATERMARK FOR `time` AS `time` - INTERVAL '0.001' SECOND
) WITH (
  'connector' = 'filesystem',
  'format' = 'flexible-json',
  'path' = 'file:/mock'
);
