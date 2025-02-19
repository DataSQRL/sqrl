CREATE TABLE Repodata (
     PRIMARY KEY (`name`, `submissionTime`) NOT ENFORCED,
     WATERMARK FOR `submissionTime` AS `submissionTime` - INTERVAL '0.001' SECOND
) WITH (
      'format' = 'flexible-json',
      'path' = '${DATA_PATH}/repodata.jsonl',
      'connector' = 'filesystem'
      );