CREATE TABLE `CompactedCdcTable` (
  `account_id`  BIGINT NOT NULL,
  `id`          BIGINT NOT NULL,
  `pt`          STRING NOT NULL,
  `op`          STRING NOT NULL,
  `payload`     STRING NOT NULL,
  `ts`          TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  `time_bucket` BIGINT NOT NULL,
  PRIMARY KEY (`account_id`) NOT ENFORCED
)
PARTITIONED BY (`time_bucket`)
WITH (
  'catalog-name' = 'mydatabase',
  'catalog-table' = 'MinCdcTable',
  'catalog-type' = 'hadoop',
  'connector' = 'iceberg',
  'format-version' = '2',
  'warehouse' = '/tmp/duckdb',
  'write.parquet.page-size-bytes' = '1000'
);
