CREATE TABLE `CdcTable` (
  `account_id`  BIGINT NOT NULL,
  `id`          BIGINT NOT NULL,
  `pt`          STRING NOT NULL,
  `op`          STRING NOT NULL,
  `payload`     STRING NOT NULL,
  `ts`          TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  `time_bucket` BIGINT NOT NULL,
  PRIMARY KEY (`account_id`, `id`) NOT ENFORCED
)
PARTITIONED BY (`account_id`, `time_bucket`)
WITH (
  'catalog-name' = 'mydatabase',
  'catalog-table' = 'CdcTable',
  'catalog-type' = 'hadoop',
  'connector' = 'iceberg',
  'format-version' = '2',
  'warehouse' = '/tmp/duckdb',
  'write.distribution-mode' = 'none',
  'write.parquet.page-size-bytes' = '1000'
);
