CREATE TABLE Likes (
     PRIMARY KEY (`_uuid`) NOT ENFORCED,
     WATERMARK FOR `event_time` AS `event_time` - INTERVAL '0.001' SECOND
) WITH (
      'format' = 'flexible-json',
      'path' = '${DATA_PATH}/likes.jsonl',
      'source.monitor-interval' = '1 sec',
      'flexible-json.timestamp-format.standard' = 'ISO-8601',
      'connector' = 'filesystem'
      )
LIKE `likes.schema.yml`;
