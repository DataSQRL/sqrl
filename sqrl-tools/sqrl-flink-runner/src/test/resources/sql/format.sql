
SET 'execution.plan-compile' = 'true';
SET 'execution.plan.output' = '/opt/flink/log/compiled.plan';

-- Define a source using datagen (auto-generates rows)
CREATE TABLE source_table (
  id INT,
  name STRING
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '500',
  'fields.id.kind' = 'sequence',
  'fields.id.start' = '1',
  'fields.id.end' = '1000'
);

-- Define a sink using print connector (prints to stdout)
CREATE TABLE sink_table (
  text STRING
) WITH (
  'connector' = 'print'
);

-- Insert into sink
INSERT INTO sink_table
SELECT concat('Completed ID: ', CAST(id as String), ' name: ', name) FROM source_table;

