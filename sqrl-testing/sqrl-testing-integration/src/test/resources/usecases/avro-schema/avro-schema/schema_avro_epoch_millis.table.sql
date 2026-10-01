CREATE TABLE `Schema` (
  PRIMARY KEY (uuidField, `timestampMillisField`) NOT ENFORCED,
  WATERMARK FOR `timestampMillisField` AS `timestampMillisField`
) WITH (
  'connector' = 'filesystem',
  'format' = 'avro',
  'avro.encoding' = 'json',
  'path' = '${DATA_PATH}/schema-epoch-millis.jsonl'
)
LIKE `schema_epoch_millis.avsc`;
