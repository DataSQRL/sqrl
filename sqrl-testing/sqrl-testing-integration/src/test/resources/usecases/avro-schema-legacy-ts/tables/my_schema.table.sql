CREATE TABLE `MySchema` (
    PRIMARY KEY (uuidField, `timestampMillisField`) NOT ENFORCED,
    WATERMARK FOR `timestampMillisField` AS `timestampMillisField`
) WITH (
    'connector' = 'filesystem',
    'format' = 'avro',
    'path' = '${DATA_PATH}/data.avro',
    'avro.timestamp_mapping.legacy' = 'true'
) LIKE `schema.avsc`;
