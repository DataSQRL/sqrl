CREATE TABLE Iceberg_sink (
     PRIMARY KEY (id, `updated_at`) NOT ENFORCED
) WITH (
      'connector' = 'iceberg',
      'catalog-table' = 'my-table',
      'warehouse' = '${ICEBERG_TEST_WAREHOUSE}',
      'catalog-type' = 'hadoop',
      'catalog-name' = 'mydatabase'
) LIKE `.`;
