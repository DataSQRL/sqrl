CREATE TABLE MyPrintSink (
  PRIMARY KEY (customerid) NOT ENFORCED
) WITH (
  'connector' = 'print'
);