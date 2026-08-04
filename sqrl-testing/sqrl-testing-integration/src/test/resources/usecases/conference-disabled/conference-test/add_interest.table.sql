CREATE TABLE AddInterest (
     PRIMARY KEY (`_uuid`) NOT ENFORCED,
     WATERMARK FOR `event_time` AS `event_time` - INTERVAL '0.001' SECOND
) WITH (
    'source.monitor-interval' = '1'
) LIKE `add_interest.jsonl`;
