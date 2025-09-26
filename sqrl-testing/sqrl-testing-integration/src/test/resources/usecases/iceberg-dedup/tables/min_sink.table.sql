CREATE TABLE `MinCdcTable` (
  `account_id`  BIGINT NOT NULL,
  `id`          BIGINT NOT NULL,
  `pt`          STRING NOT NULL,
  `op`          STRING NOT NULL,
  `payload`     STRING NOT NULL,
  `ts`          TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  `time_bucket` BIGINT NOT NULL,
  PRIMARY KEY (`account_id`) NOT ENFORCED
)
PARTITIONED BY (`account_id`, `time_bucket`)
WITH (
  'connector' = 'iceberg',
  'catalog-name' = 'mycatalog',
  'catalog-database' = 'mydatabase',
  'catalog-table' = 'mincdctable',
  'catalog-type' = 'hadoop',
  'warehouse' = '/tmp/duckdb',
  'format-version' = '2'
);
