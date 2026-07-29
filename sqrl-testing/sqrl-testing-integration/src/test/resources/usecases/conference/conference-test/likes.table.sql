CREATE TABLE Likes (
     PRIMARY KEY (`_uuid`) NOT ENFORCED,
     WATERMARK FOR `event_time` AS `event_time` - INTERVAL '0.001' SECOND
) WITH (
    'flexible-json.timestamp-format.standard' = 'ISO-8601',
    'source.monitor-interval' = '1 sec'
) LIKE `likes.jsonl`;
