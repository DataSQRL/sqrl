CREATE TABLE Orders (
  id BIGINT NOT NULL,
  customer_id BIGINT NOT NULL,
  amount DOUBLE NOT NULL,
  ordered_at TIMESTAMP_LTZ(3) NOT NULL,
  WATERMARK FOR ordered_at AS ordered_at - INTERVAL '1' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///tmp/contracts-compile/orders',
  'format' = 'flexible-json'
);
