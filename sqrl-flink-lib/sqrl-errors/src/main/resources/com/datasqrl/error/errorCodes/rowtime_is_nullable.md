The rowtime column is used to advance the watermark and for time-based operations. When it is null, the watermark does not advance and the record can be ignored for time-based operations, leading to unexpected output and performance issues.

Ensure that the rowtime column always has a value by filtering or setting a default. If that is the case but the compiler doesn't recognize it, explicitly cast it in the table definition.
For example:
`rowTime AS COALESCE(TO_TIMESTAMP_LTZ(lastUpdated, 0), TIMESTAMP '1970-01-01 00:00:00.000')`