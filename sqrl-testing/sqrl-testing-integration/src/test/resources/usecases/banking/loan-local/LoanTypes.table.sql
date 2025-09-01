CREATE TABLE LoanTypes (
PRIMARY KEY (id, `updated_at`) NOT ENFORCED,
WATERMARK FOR `updated_at` AS `updated_at` - INTERVAL '0.001' SECOND
) WITH (
'format' = 'flexible-json',
'path' = '${DATA_PATH}/loan_types.jsonl',
'source.monitor-interval' = '10 sec',
'connector' = 'filesystem'
)
LIKE `LoanTypes.schema.yml`;
