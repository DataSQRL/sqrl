This table has multiple ROWTIME columns, which makes the authoritative timestamp for its records ambiguous.
This can cause confusion for downstream users and unexpected data processing results.

Keep the authoritative ROWTIME column unchanged and `CAST` all other ROWTIME columns to regular timestamps.
For example, to retain `col_b` as ROWTIME while keeping `col_a` as a regular timestamp:
```
CAST(col_a AS TIMESTAMP_LTZ(3)) AS regular_timestamp,
col_b AS event_time
```

For non-temporal joins, the authoritative timestamp is usually the latest event time from all joined tables:
```
GREATEST(t1.eventTime, t2.eventTime) AS eventTime
```
