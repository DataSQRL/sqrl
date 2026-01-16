CREATE TABLE events (
  user_id INT,
  ts_ms BIGINT,
  ts AS TO_TIMESTAMP_LTZ(ts_ms, 3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector'='datagen',
  'rows-per-second'='5',
  'fields.user_id.min'='1', 'fields.user_id.max'='3',
  'fields.ts_ms.kind'='sequence', 'fields.ts_ms.start'='0', 'fields.ts_ms.end'='1000'
);

CREATE TABLE dim_users (
  user_id INT,
  segment STRING
) WITH (
  'connector'='datagen',
  'rows-per-second'='1',
  'fields.user_id.min'='1', 'fields.user_id.max'='3',
  'fields.segment.length'='5'
);

SELECT e.user_id
FROM (SELECT user_id, ts FROM events) AS e
         JOIN dim_users AS d
              ON e.user_id = d.user_id
WHERE e.ts > TO_TIMESTAMP_LTZ(1000, 3);
