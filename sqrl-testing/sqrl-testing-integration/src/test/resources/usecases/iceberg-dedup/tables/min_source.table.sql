CREATE TABLE SrcRecords (
    WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
) WITH (
    'source.monitor-interval' = '10 sec'
) LIKE `min_source_records.jsonl`;
