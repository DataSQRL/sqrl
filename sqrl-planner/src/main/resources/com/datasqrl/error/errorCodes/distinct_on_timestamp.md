The sort order for the DISTINCT expression is not a timestamp order which can result in inefficient processing.

If possible, try to use the timestamp column of the table as the sort order. This allows the stream engine to process the deduplication more efficiently.

For tables defined with `CREATE TABLE`, the timestamp column is the column with the `WATERMARK` specification. See: https://docs.datasqrl.com/docs/connectors

For other tables, the timestamp column is inferred based on the source tables.