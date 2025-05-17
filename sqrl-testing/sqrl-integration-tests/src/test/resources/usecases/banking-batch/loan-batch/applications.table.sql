CREATE TABLE Applications (
PRIMARY KEY (id, `updated_at`) NOT ENFORCED
) WITH (
'format' = 'flexible-json',
'path' = '${DATA_PATH}/applications.jsonl',
'connector' = 'filesystem'
);