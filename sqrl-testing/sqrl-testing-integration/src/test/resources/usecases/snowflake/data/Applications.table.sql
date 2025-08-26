CREATE TABLE Applications (
     PRIMARY KEY (`id`, `updated_at`) NOT ENFORCED,
     WATERMARK FOR `updated_at` AS `updated_at` - INTERVAL '0.001' SECOND
) WITH (
      'format' = 'flexible-json',
      'path' = '${DATA_PATH}/applications.jsonl',
      'source.monitor-interval' = '1 sec',
      'connector' = 'filesystem'
      );