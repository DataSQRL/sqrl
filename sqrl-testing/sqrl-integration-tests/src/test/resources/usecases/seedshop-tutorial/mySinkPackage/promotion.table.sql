CREATE TABLE SinkTable (
  PRIMARY KEY (customerid) NOT ENFORCED
) WITH (
      'format' = 'flexible-json',
      'path' = '/tmp/sink/',
      'connector' = 'filesystem'
) LIKE `.`;