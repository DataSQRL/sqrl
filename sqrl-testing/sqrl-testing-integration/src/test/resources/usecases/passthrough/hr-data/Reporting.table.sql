CREATE TABLE `reporting` (
  `employeeid` BIGINT NOT NULL,
  `managerid` BIGINT NOT NULL,
  `updatedDate` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  WATERMARK FOR `updatedDate` AS `updatedDate` - INTERVAL '0.001' SECOND
) WITH (
  'connector' = 'filesystem',
  'format' = 'flexible-json',
  'path' = '${DATA_PATH}/reporting.jsonl',
  'source.monitor-interval' = '10 sec'
);