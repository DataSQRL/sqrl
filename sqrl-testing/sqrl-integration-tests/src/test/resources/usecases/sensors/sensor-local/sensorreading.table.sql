CREATE TABLE SensorReading (
     `timestamp` AS EpochMilliToTimestamp(`time`),
     PRIMARY KEY (sensorid, `time`) NOT ENFORCED,
     WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '0.001' SECOND
) WITH (
      'format' = 'flexible-csv',
      'path' = '${DATA_PATH}/sensorreading.csv.gz',
      'connector' = 'filesystem',
      'flexible-csv.skip-header' = 'true'
      );