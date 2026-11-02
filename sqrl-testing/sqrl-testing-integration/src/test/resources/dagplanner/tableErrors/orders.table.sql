CREATE TABLE Orders (
  id BIGINT,
  order_time TIMESTAMP_LTZ(3),
  WATERMARK FOR ordertime AS ordertime - INTERVAL '1' SECOND
) WITH (
  'connector' = 'datagen'
);
