CREATE TABLE Customers (
    `timestamp` AS EpochMilliToTimestamp(`changed_on`),
    PRIMARY KEY (`id`, `changed_on`) NOT ENFORCED,
    WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '0.001' SECOND
) WITH (
    'format' = 'flexible-json',
    'path' = '${DATA_PATH}/customers.jsonl',
    'connector' = 'filesystem'
);