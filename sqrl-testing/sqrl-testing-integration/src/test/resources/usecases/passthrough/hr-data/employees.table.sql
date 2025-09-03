CREATE TABLE `Employees` (
  `employeeid` BIGINT NOT NULL,
  `name` VARCHAR(2147483647) CHARACTER SET `UTF-16LE` NOT NULL,
  `email` VARCHAR(2147483647) CHARACTER SET `UTF-16LE` NOT NULL,
  `updatedDate` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  WATERMARK FOR `updatedDate` AS `updatedDate` - INTERVAL '0.001' SECOND
) WITH (
  'connector' = 'filesystem',
  'format' = 'flexible-json',
  'path' = '${DATA_PATH}/employees.jsonl',
  'source.monitor-interval' = '10 sec'
);