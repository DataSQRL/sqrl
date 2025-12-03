CREATE TABLE events (
  `id` INT NOT NULL,
  `name` STRING NOT NULL,
  `event_time` TIMESTAMP(3),
  PRIMARY KEY (`id`) NOT ENFORCED,
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '0.001' SECOND
) WITH (
  'format' = 'json',
  'path' = '${DATA_PATH}/events.jsonl',
  'connector' = 'filesystem'
);
