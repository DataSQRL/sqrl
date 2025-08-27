CREATE TABLE Events (
     PRIMARY KEY (url, last_updated) NOT ENFORCED,
     WATERMARK FOR last_updated AS last_updated - INTERVAL '0.001' SECOND
) WITH (
      'format' = 'json',
      'path' = '${DATA_PATH}/events.jsonl',
      'json.timestamp-format.standard' = 'ISO-8601',
      'source.monitor-interval' = '1 sec',
      'connector' = 'filesystem'
      )
LIKE `Events.schema.yml`;
