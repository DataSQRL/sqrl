CREATE TABLE Product (
  PRIMARY KEY (productid, name, description, category) NOT ENFORCED,
  WATERMARK FOR `_ingest_time` AS `_ingest_time` - INTERVAL '0.001' SECOND
) WITH (
  'format' = 'flexible-json',
  'path' = 'file:/mock',
  'source.monitor-interval' = '10 sec',
  'connector' = 'filesystem'
  )
LIKE `Product.schema.yml`;
