CREATE TABLE Orders (
    `_source_time` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
    WATERMARK FOR `_source_time` AS `_source_time`
) WITH (
    'format' = 'avro',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'datasqrl-orders',
    'topic' = '${sqrl:topic}',
    'connector' = 'kafka',
    'avro.timestamp_mapping.legacy' = 'false'
)
LIKE `Orders.avsc`;
