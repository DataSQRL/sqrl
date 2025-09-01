CREATE TABLE Content (
   PRIMARY KEY (url, updated) NOT ENFORCED,
   WATERMARK FOR `updated` AS `updated` - INTERVAL '0.001' SECOND
) WITH (
      'format' = 'flexible-json',
      'path' = '${DATA_PATH}/content.jsonl',
      'source.monitor-interval' = '10 sec',
      'connector' = 'filesystem'
      )
LIKE `Content.schema.yml`;
