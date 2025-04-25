CREATE TABLE Customers (
    `timestamp` AS TO_TIMESTAMP_LTZ(`changed_on`,3),
    PRIMARY KEY (`id`, `changed_on`) NOT ENFORCED,
    WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '0.001' SECOND
) WITH (
    'format' = 'flexible-json',
    'path' = '${DATA_PATH}/customers.jsonl',
    'connector' = 'filesystem'
);