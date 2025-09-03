CREATE TABLE Json (
     PRIMARY KEY (id) NOT ENFORCED,
     WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '0.001' SECOND
) WITH (
      'format' = 'flexible-json',
      'path' = '${DATA_PATH}/json.jsonl',
      'connector' = 'filesystem'
) LIKE `json.schema.yml`;
