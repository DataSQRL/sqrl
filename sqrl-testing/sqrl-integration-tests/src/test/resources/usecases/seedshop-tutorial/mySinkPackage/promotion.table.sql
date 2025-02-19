CREATE TABLE SinkTable (
) WITH (
      'format' = 'flexible-json',
      'path' = '/tmp/sink/',
      'connector' = 'filesystem'
      );