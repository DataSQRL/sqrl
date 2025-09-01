CREATE TABLE Customers (
PRIMARY KEY (id, `updated_at`) NOT ENFORCED
) WITH (
'format' = 'flexible-json',
'path' = '${DATA_PATH}/customers.jsonl',
'connector' = 'filesystem'
)
LIKE `Customers.schema.yml`;
