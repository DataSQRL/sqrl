CREATE TABLE Orders (
     `_ingest_time` AS PROCTIME(),
     PRIMARY KEY (id, customerid, `time`) NOT ENFORCED
) WITH (
      'format' = 'flexible-json',
      'path' = '${DATA_PATH}/orders.jsonl',
      'source.monitor-interval' = '10000',
      'connector' = 'filesystem'
      )
LIKE `orders.schema.yml`;
