CREATE TABLE Alerts (
  `id` BIGINT NOT NULL,
  `level` NOT_A_TYPE NOT NULL
) WITH (
  'connector' = 'filesystem',
  'format' = 'flexible-json',
  'path' = 'file:/mock'
);
