CREATE TABLE Numbers (
    val INT NOT NULL
) WITH (
    'connector' = 'datagen',
    'number-of-rows' = '5',
    'rows-per-second' = '5',
    'fields.val.kind' = 'random',
    'fields.val.min' = '1',
    'fields.val.max' = '5'
);
