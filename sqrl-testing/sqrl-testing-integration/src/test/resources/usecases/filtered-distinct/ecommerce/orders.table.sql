CREATE TABLE Orders (
     `_ingest_time` AS PROCTIME(),
     PRIMARY KEY (id, customerid, `time`) NOT ENFORCED
) WITH (
      'source.monitor-interval' = '10000'
) LIKE `orders.jsonl`;
