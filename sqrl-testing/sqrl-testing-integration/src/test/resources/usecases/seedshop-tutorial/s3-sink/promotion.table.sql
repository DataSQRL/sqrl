CREATE TABLE SinkTable (
  PRIMARY KEY (customerid) NOT ENFORCED
) WITH (
      'connector' = 'filesystem',
      'format' = 'flexible-json',
      'path' = 's3://test/'
);
