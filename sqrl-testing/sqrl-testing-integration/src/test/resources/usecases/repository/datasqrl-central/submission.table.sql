CREATE TABLE `Submission` (
  `event_time` AS NOW(),
  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '0.001' SECOND
) WITH (
  'source.monitor-interval' = '10 sec'
) LIKE `submission.jsonl`;
