CREATE TABLE Schema (
PRIMARY KEY (uuidField, `timestampMillisField`) NOT ENFORCED,
WATERMARK FOR `timestampMillisField` AS `timestampMillisField`
) WITH (
'format' = 'flexible-json',
'path' = '${DATA_PATH}/schema.jsonl',
'connector' = 'filesystem'
);