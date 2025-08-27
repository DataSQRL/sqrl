CREATE TABLE `Schema` (
PRIMARY KEY (uuidField, `timestampMillisField`) NOT ENFORCED,
WATERMARK FOR `timestampMillisField` AS `timestampMillisField`
) WITH (
'format' = 'avro',
'avro.encoding' = 'json',
'path' = '${DATA_PATH}/schema.jsonl',
'connector' = 'filesystem'
)
LIKE `schema.avsc`;
