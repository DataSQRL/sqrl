CREATE TABLE MachineGroup (
     PRIMARY KEY (groupId, `created`) NOT ENFORCED,
     WATERMARK FOR `created` AS `created` - INTERVAL '0.001' SECOND
) WITH (
      'format' = 'flexible-json',
      'path' = '${DATA_PATH}/machinegroup.jsonl',
      'connector' = 'filesystem'
      );