CREATE TABLE `Employees` (
  WATERMARK FOR `updatedDate` AS `updatedDate` - INTERVAL '0.001' SECOND
) WITH (
  'source.monitor-interval' = '10 sec'
) LIKE `employees.jsonl`;