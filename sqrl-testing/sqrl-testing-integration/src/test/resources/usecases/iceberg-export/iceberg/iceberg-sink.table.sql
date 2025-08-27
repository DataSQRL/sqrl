CREATE TABLE MyTable (
     PRIMARY KEY (id, `updated_at`) NOT ENFORCED
) WITH (
      'connector' = 'iceberg',
      'catalog-table' = 'my-table',
      'warehouse' = '/tmp/duckdb',
      'catalog-type' = 'hadoop',
      'catalog-name' = 'mydatabase'
) LIKE `.`;