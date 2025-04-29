CREATE TABLE SensorReading (
     `timestamp` AS COALESCE(TO_TIMESTAMP_LTZ(`time`, 3), TIMESTAMP '1970-01-01 00:00:00.000'),
     PRIMARY KEY (sensorid, `time`) NOT ENFORCED,
     WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '0.001' SECOND
) WITH (
      'format' = 'flexible-csv',
      'path' = '${DATA_PATH}/sensorreading.csv.gz',
      'connector' = 'filesystem',
      'flexible-csv.skip-header' = 'true'
      );