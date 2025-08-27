CREATE TABLE Orders (
    `_ingest_time` AS PROCTIME(),
    PRIMARY KEY (id, `time`) NOT ENFORCED
) WITH (
'format' = 'flexible-json',
'path' = 'file:/mock',
'source.monitor-interval' = '10 sec',
'connector' = 'filesystem'
)
LIKE `Orders.schema.yml`;
