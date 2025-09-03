CREATE TABLE OrderItems (
     PRIMARY KEY (`id`, `time`) NOT ENFORCED,
     WATERMARK FOR `time` AS `time` - INTERVAL '0.001' SECOND
) WITH (
      'format' = 'flexible-json',
      'path' = '${DATA_PATH}/order_items.jsonl',
      'connector' = 'filesystem'
      )
LIKE `order_items.schema.yml`;
