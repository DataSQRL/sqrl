CREATE TABLE ApplicationUpdates (
PRIMARY KEY (loan_application_id, `event_time`) NOT ENFORCED
) WITH (
'format' = 'flexible-json',
'path' = '${DATA_PATH}/application_updates.jsonl',
'connector' = 'filesystem'
)
LIKE `application_updates.schema.yml`;
