CREATE TABLE Schema (
PRIMARY KEY (uuidField, `timestampMillisField`) NOT ENFORCED,
WATERMARK FOR `timestampMillisField` AS `timestampMillisField`
) WITH (
'format' = 'json',
'json.timestamp-format.standard' = 'ISO-8601',
'path' = '${DATA_PATH}/schema.jsonl',
'connector' = 'filesystem'
);