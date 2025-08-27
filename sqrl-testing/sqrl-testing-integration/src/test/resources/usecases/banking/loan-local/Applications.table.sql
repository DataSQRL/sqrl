CREATE TABLE Applications (
PRIMARY KEY (id, `updated_at`) NOT ENFORCED,
WATERMARK FOR `updated_at` AS `updated_at` - INTERVAL '0.001' SECONDS
) WITH (
'format' = 'flexible-json',
'path' = '${DATA_PATH}/applications.jsonl',
'source.monitor-interval' = '10 sec',
'connector' = 'filesystem'
)
LIKE `Applications.schema.yml`;
