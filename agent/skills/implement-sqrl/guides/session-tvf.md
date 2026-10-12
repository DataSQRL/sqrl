# How to use SESSION Window TVF Issues

**Key Notes**:
* SESSION TVF requires a `PARTITION BY` clause.
* The `PARTITION BY` clause can only be one column.
* Window deduplication is not supported by SESSION TVF yet
* Like every window TVF, SESSION exposes the window as the columns `window_start`, `window_end` and `window_time`. Select and group by those; the legacy group-window functions `session_start(...)` / `session_end(...)` do not exist inside a TVF query and fail to compile.

## PARTITION BY

The SESSION Table-Valued Function (TVF) requires a `PARTITION BY` clause which contains the non-timestamp group by keys, even if they are already in the GROUP BY clause. Otherwise, it becomes a retraction stream.

```sql
-- Incorrect: Missing PARTITION BY
SELECT
  user_id,
  window_start,
  window_end,
  window_time,
  COUNT(*) AS event_count
FROM SESSION(TABLE events, DESCRIPTOR(ts_ltz), INTERVAL '30' MINUTES)
GROUP BY user_id, window_start, window_end, window_time;

-- Correct: Include PARTITION BY
SELECT
  user_id,
  window_start,
  window_end,
  window_time,
  COUNT(*) AS event_count
FROM SESSION(
  TABLE events PARTITION BY user_id,  -- Required!
  DESCRIPTOR(ts_ltz),
  INTERVAL '30' MINUTES)
GROUP BY user_id, window_start, window_end, window_time;
```

### Composite Column Partitioning

Currently, the `PARTITION BY` clause can only contain a single column. If you need to partition by multiple columns, concatenate them upfront:

```sql
-- Create a composite partition key
_EventsWithKey := SELECT
    CONCAT(user_id, ':', device_id) AS partition_key,
    *
  FROM events;

SELECT
  partition_key,
  window_start,
  window_end,
  window_time,
  COUNT(*) AS event_count
FROM SESSION(
  TABLE _EventsWithKey PARTITION BY partition_key,
  DESCRIPTOR(ts_ltz),
  INTERVAL '30' MINUTES)
GROUP BY partition_key, window_start, window_end, window_time;
```
