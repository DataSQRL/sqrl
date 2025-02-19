CREATE TABLE MyTable (
     PRIMARY KEY (id, `updated_at`) NOT ENFORCED,
     WATERMARK FOR `updated_at` AS `updated_at` - INTERVAL '0.001' SECOND
) WITH (
      'connector' = 'iceberg',
      'catalog-table' = 'my-table',
      'warehouse' = '/tmp/duckdb',
      'catalog-type' = 'hadoop',
      'catalog-name' = 'mydatabase'
      );