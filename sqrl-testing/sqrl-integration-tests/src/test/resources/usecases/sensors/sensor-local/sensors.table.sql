CREATE TABLE Sensors (
     `timestamp` AS TO_TIMESTAMP_LTZ(`placed`, 3),
     PRIMARY KEY (id, placed) NOT ENFORCED,
     WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '0.001' SECOND
) WITH (
      'format' = 'flexible-json',
      'path' = '${DATA_PATH}/sensors.jsonl',
      'connector' = 'filesystem'
      );