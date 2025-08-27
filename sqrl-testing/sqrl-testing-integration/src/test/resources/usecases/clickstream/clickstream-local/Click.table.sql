CREATE TABLE Click (
     PRIMARY KEY (url, userid, `timestamp`) NOT ENFORCED,
     WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '1' SECOND
) WITH (
      'format' = 'flexible-json',
      'path' = '${DATA_PATH}/click.jsonl',
      'source.monitor-interval' = '10 sec',
      'connector' = 'filesystem'
      );