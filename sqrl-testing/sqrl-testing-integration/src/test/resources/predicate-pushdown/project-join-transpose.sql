CREATE TABLE events (
  user_id INT,
  extra INT
) WITH (
  'connector'='datagen',
  'rows-per-second'='5',
  'fields.user_id.min'='1', 'fields.user_id.max'='3',
  'fields.extra.min'='0', 'fields.extra.max'='1'
);

CREATE TABLE dim_users_hist (
  user_id INT,
  segment STRING
) WITH (
  'connector'='datagen',
  'rows-per-second'='1',
  'fields.user_id.min'='1', 'fields.user_id.max'='3',
  'fields.segment.length'='5'
);

SELECT e.user_id + 1 AS uid_score, CHAR_LENGTH(u.segment) AS segment_len
FROM events AS e JOIN dim_users_hist AS u ON e.user_id = u.user_id;
