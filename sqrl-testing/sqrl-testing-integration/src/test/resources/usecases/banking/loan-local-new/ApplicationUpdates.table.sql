CREATE TABLE ApplicationUpdates (
PRIMARY KEY (loan_application_id, `event_time`) NOT ENFORCED,
WATERMARK FOR `event_time` AS `event_time`
) WITH (
'format' = 'flexible-json',
'path' = '${DATA_PATH}/application_updates.jsonl',
'connector' = 'filesystem'
);