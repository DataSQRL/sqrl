CREATE TABLE Customers (
    `timestamp` AS COALESCE(TO_TIMESTAMP_LTZ(`changed_on`, 3), TIMESTAMP '1970-01-01 00:00:00.000'),
    PRIMARY KEY (`id`, `changed_on`) NOT ENFORCED,
    WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '0.001' SECOND
) WITH (
    'format' = 'flexible-json',
    'path' = '${DATA_PATH}/customers.jsonl',
    'connector' = 'filesystem'
)
LIKE `customers.schema.yml`;
