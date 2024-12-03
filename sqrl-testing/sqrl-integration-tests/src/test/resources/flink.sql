CREATE TABLE fake0 (
 name STRING,
 metric INT NOT NULL,
 label STRING,
 seq BIGINT,
 noise BIGINT,
 ts AS PROCTIME()
) WITH (
      'connector' = 'datagen',
    'rows-per-second' = '1',
    'fields.metric.min' = '0',
    'fields.metric.max' = '1',
    'fields.name.length' = '1',
    'fields.label.length' = '6',
    'fields.seq.kind' = 'sequence',
    'fields.seq.start' = '1000',
    'fields.seq.end' = '10000',
    'fields.noise.min' = '-4',
    'fields.noise.max' = '4'
    );

CREATE VIEW fake1 AS
    SELECT name, metric, seq + noise AS cdc, label, ts FROM fake0;

CREATE VIEW Old_cdc AS
    SELECT name, metric, label, cdc, ts,
           max(cdc) over (partition by name order by ts) max_cdc
           FROM fake1;


CREATE VIEW Current_and_previous AS
   select
        name,
        metric,
        label,
        cdc,
        ts,
        lag(metric, 1) over (partition by name order by ts) previous_metric
    from Old_cdc WHERE cdc >= max_cdc;

CREATE VIEW Filtered AS
select name, metric, label, cdc, ts
from Current_and_previous WHERE (previous_metric IS NULL OR previous_metric <> metric);

CREATE VIEW Dedup1 AS
SELECT name, metric, cdc, ts
FROM (
         SELECT *,
                ROW_NUMBER() OVER (PARTITION BY name ORDER BY cdc DESC) as rn
         FROM Filtered
     )
WHERE rn = 1;

CREATE VIEW Dedup2 AS
SELECT name, metric, cdc
FROM (
         SELECT *,
                ROW_NUMBER() OVER (PARTITION BY name ORDER BY cdc DESC) as rn
         FROM fake1
     )
WHERE rn = 1;




CREATE VIEW DedupInner AS
SELECT name, metric, ts
FROM (
         SELECT *,
                ROW_NUMBER() OVER (PARTITION BY name, metric ORDER BY ts DESC) as rn
         FROM fake1
     )
WHERE rn = 1;

CREATE VIEW Dedup AS
SELECT name, metric, ts
FROM (
         SELECT *,
                ROW_NUMBER() OVER (PARTITION BY name ORDER BY ts DESC) as rn
         FROM fake1
     )
WHERE rn = 1;


CREATE VIEW AggregatedStocks AS
SELECT name, SUM(metric) AS metric, window_time AS ts
FROM TABLE(
        TUMBLE(TABLE Filtered, DESCRIPTOR(ts), INTERVAL '4' SECONDS))
GROUP BY name, window_start, window_end, window_time;



CREATE TABLE PrintSinkFilter (
   name STRING,
   metric double,
   cdc BIGINT,
   ts TIMESTAMP
) WITH (
  'connector' = 'print',
  'print-identifier' = 'filter'
  );

CREATE TABLE PrintSinkDedup (
     name STRING,
     metric double,
     cdc BIGINT,
     ts TIMESTAMP
) WITH (
      'connector' = 'print',
      'print-identifier' = 'dedup'
      );


CREATE TABLE InputData (
   name STRING,
   metric double,
   cdc BIGINT,
   ts TIMESTAMP
) WITH (
  'connector' = 'print',
  'print-identifier' = 'in'
  );
