CREATE TABLE `CompactionTracker` (
  `table_name`      STRING NOT NULL,
  `job_id`          STRING NOT NULL,
  `partition_id`    BIGINT NOT NULL,
  `cur_time_bucket` BIGINT NOT NULL,
  `max_time_bucket` BIGINT NOT NULL,
  `action`          STRING NOT NULL,
  `action_time`     TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL
)
PARTITIONED BY (`table_name`)
WITH (
  'connector' = 'iceberg',
  'catalog-name' = 'mycatalog',
  'catalog-database' = 'mydatabase',
  'catalog-table' = 'compactiontracker',
  'catalog-type' = 'hadoop',
  'warehouse' = '/tmp/duckdb',
  'format-version' = '2'
);
