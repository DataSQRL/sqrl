
CREATE TEMPORARY FUNCTION IF NOT EXISTS `uuid` AS 'com.datasqrl.secure.Uuid' LANGUAGE JAVA;

CREATE TEMPORARY TABLE `orders` (
  `_source_time` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL METADATA FROM 'value.source.timestamp',
  `id` BIGINT NOT NULL,
  `customerid` BIGINT NOT NULL,
  `time` VARCHAR(2147483647) CHARACTER SET `UTF-16LE` NOT NULL,
  `entries` ROW(`productid` INTEGER NOT NULL, `quantity` INTEGER NOT NULL, `unit_price` DOUBLE NOT NULL, `discount` DOUBLE) NOT NULL ARRAY NOT NULL,
  `uuid` AS uuid(),
  PRIMARY KEY (`id`) NOT ENFORCED,
  WATERMARK FOR `_source_time` AS (`_source_time` - INTERVAL '0.001' SECONDS)
) WITH (
  'connector' = 'kafka',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'groupid',
  'format'  = 'avro',
  'topic' = 'topic'
)