The rowtime (event time) column of the table is nullable.

The rowtime column advances the watermark and drives time-based operations. Records with a null rowtime do not advance the watermark and can be dropped by time-based operations, which leads to unexpected results and performance issues.

Erroneous code example:
```sql
CREATE TABLE Clicks (
  url STRING,
  event_time TIMESTAMP_LTZ(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH ( ... );
```

Declare the rowtime column `NOT NULL`, or make sure it always has a value by filtering or by setting a default for computed columns, e.g. `event_time AS COALESCE(TO_TIMESTAMP_LTZ(ts_millis, 3), TO_TIMESTAMP_LTZ(0, 3))`:
```sql
CREATE TABLE Clicks (
  url STRING,
  event_time TIMESTAMP_LTZ(3) NOT NULL,
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH ( ... );
```
