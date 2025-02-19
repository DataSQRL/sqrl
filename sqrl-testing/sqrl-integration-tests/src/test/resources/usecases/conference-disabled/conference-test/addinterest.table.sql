CREATE TABLE AddInterest (
     PRIMARY KEY (`_uuid`) NOT ENFORCED,
     WATERMARK FOR `event_time` AS `event_time` - INTERVAL '0.001' SECOND
) WITH (
      'format' = 'flexible-json',
      'path' = '${DATA_PATH}/addinterest.jsonl',
      'source.monitor-interval' = '1',
      'connector' = 'filesystem'
      );