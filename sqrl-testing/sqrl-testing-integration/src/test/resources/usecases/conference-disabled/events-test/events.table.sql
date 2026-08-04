CREATE TABLE Events (
    PRIMARY KEY (url, last_updated) NOT ENFORCED,
    WATERMARK FOR last_updated AS last_updated - INTERVAL '0.001' SECOND
) WITH (
    'connector' = 'filesystem',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'path' = '${DATA_PATH}/events.jsonl',
    'source.monitor-interval' = '1 sec'
) LIKE `events.jsonl`;
