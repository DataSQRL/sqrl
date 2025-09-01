CREATE TABLE ApplicationUpdates (
PRIMARY KEY (loan_application_id, `event_time`) NOT ENFORCED
) WITH (
'format' = 'flexible-json',
'path' = '${DATA_PATH}/applicationupdates.jsonl',
'connector' = 'filesystem'
)
LIKE `ApplicationUpdates.schema.yml`;
