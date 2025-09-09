CREATE TABLE Sensors (
     `timestamp` AS COALESCE(TO_TIMESTAMP_LTZ(`placed`, 3), TIMESTAMP '1970-01-01 00:00:00.000'),
     PRIMARY KEY (id, placed) NOT ENFORCED,
     WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '0.001' SECOND
) WITH (
      'format' = 'flexible-json',
      'path' = '${DATA_PATH}/sensors.jsonl',
      'connector' = 'filesystem'
      )
LIKE `sensors.schema.yml`;
