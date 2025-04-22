CREATE TABLE Sensors (
     `timestamp` AS EpochMilliToTimestamp(`placed`),
     PRIMARY KEY (id, placed) NOT ENFORCED,
     WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '0.001' SECOND
) WITH (
      'format' = 'flexible-json',
      'path' = '${DATA_PATH}/sensors.jsonl',
      'connector' = 'filesystem'
      );