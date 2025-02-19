CREATE TABLE Data (
    `ID` BIGINT NOT NULL,
    `EPOCH_TIMESTAMP` BIGINT NOT NULL,
    `SOME_VALUE` STRING NOT NULL,
    `_uuid` STRING NOT NULL,
    `TIMESTAMP` AS time.epochMilliToTimestamp(`EPOCH_TIMESTAMP`),
    `event_time` AS time.epochMilliToTimestamp(`EPOCH_TIMESTAMP`),
    PRIMARY KEY (`ID`) NOT ENFORCED,
    WATERMARK FOR `TIMESTAMP` AS `TIMESTAMP` - INTERVAL '0.001' SECOND
) WITH (
    'connector' = 'datagen',
    'number-of-rows' = '10',
    'fields.ID.kind' = 'sequence',
    'fields.ID.start' = '0',
    'fields.ID.end' = '9',
    'fields.EPOCH_TIMESTAMP.kind' = 'sequence',
    'fields.EPOCH_TIMESTAMP.start' = '1719318565000',
    'fields.EPOCH_TIMESTAMP.end' = '1719319565000',
    'fields.SOME_VALUE.kind' = 'random',
    'fields._uuid.kind' = 'random'
);