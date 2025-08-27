CREATE TABLE Products (
     PRIMARY KEY (`id`, `updated`) NOT ENFORCED,
     WATERMARK FOR `updated` AS `updated` - INTERVAL '0.001' SECOND
) WITH (
      'format' = 'flexible-json',
      'path' = '${DATA_PATH}/products.jsonl',
      'connector' = 'filesystem'
      )
LIKE `Products.schema.yml`;
