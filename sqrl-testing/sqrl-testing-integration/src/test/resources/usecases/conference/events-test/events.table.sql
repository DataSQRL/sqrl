CREATE TABLE Events (
    PRIMARY KEY (url, last_updated) NOT ENFORCED,
    WATERMARK FOR last_updated AS last_updated - INTERVAL '0.001' SECOND
) WITH (
    'source.monitor-interval' = '1 sec'
) LIKE `events.jsonl`;
