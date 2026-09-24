CREATE TABLE `userinfo` (
  `userid` BIGINT NOT NULL,
  `orgid` BIGINT NOT NULL,
  `last_updated` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  `event_time` AS NOW(),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '0.001' SECOND
) WITH (
  'connector' = 'filesystem',
  'format' = 'flexible-json',
  'flexible-json.timestamp-format.standard' = 'ISO-8601',
  'path' = '${DATA_PATH}/userinfo.jsonl',
  'source.monitor-interval' = '10 sec'
);
