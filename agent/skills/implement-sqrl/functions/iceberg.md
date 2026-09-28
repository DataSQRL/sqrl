# Iceberg Functions

| Function | Description |
|----------|-------------|
| `read_partition_sizes(String warehouse, String catalogType, String catalogName, String databaseName, String tableName)` | Table function returning `ROW<partition_map MAP<STRING, STRING>, partition_size BIGINT>` for each Iceberg partition. |
| `delete_duplicated_data(String warehouse, String catalogType, String catalogName, String databaseName, String tableName, Long maxTimeBucket, MULTISET<MAP<STRING, STRING>> partitionSet)` | Deletes duplicate data up to a time bucket for the listed partition specifications; returns whether the deletion succeeded. |
