CREATE TABLE Orders (
    `id` BIGINT NOT NULL,
    `customerid` BIGINT NOT NULL,
    `time` TIMESTAMP NOT NULL,
    `entries` ARRAY<ROW<`productid` BIGINT NOT NULL, `quantity` BIGINT NOT NULL, `unit_price` DOUBLE NOT NULL, `discount` DOUBLE> NOT NULL> NOT NULL,
    `_ingest_time` AS PROCTIME(),
    PRIMARY KEY (`id`, `time`) NOT ENFORCED
) WITH (
    'connector' = 'filesystem',
    'format' = 'flexible-json',
    'path' = 'file:/mock',
    'source.monitor-interval' = '10 sec'
);
