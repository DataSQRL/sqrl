The sort order for the DISTINCT expression is not a timestamp order which can result in inefficient processing.

If possible, try to use the timestamp column of the table as the sort order. This allows the stream engine to process the deduplication more efficiently.

For imported tables, the timestamp column is defined in `table.json` configuration file. See: https://www.datasqrl.com/sqrl/connectors/#table-configuration

For other tables, the timestamp column is inferred based on the source tables.