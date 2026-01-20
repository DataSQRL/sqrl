CREATE TABLE events (
  user_id INT,
  amount INT
) WITH (
  'connector'='datagen',
  'rows-per-second'='5',
  'fields.user_id.min'='1', 'fields.user_id.max'='3',
  'fields.amount.min'='0', 'fields.amount.max'='100'
);

CREATE TABLE dim_users (
  user_id INT,
  segment STRING
) WITH (
  'connector'='datagen',
  'rows-per-second'='1',
  'fields.user_id.min'='1', 'fields.user_id.max'='3',
  'fields.segment.length'='5'
);

SELECT e.user_id
FROM (SELECT user_id, amount + 1 AS amount_plus FROM events) AS e
         JOIN dim_users AS d
              ON e.user_id = d.user_id
WHERE e.amount_plus > 10;
