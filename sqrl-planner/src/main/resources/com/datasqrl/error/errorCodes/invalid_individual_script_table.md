The table that an `EXPORT` targets in another script is neither an external table nor an engine-managed table.

Erroneous code example:
```sql
-- connectors/sinks.sqrl
CREATE TABLE OrderExport (
  id BIGINT NOT NULL,
  amount DOUBLE
);

-- main script
EXPORT HighValueOrders TO connectors.sinks.OrderExport;
```

Give the target table a `connector` table option (external table) or an `engine(...)` hint (internal table):
```sql
/*+ engine(iceberg) */
CREATE TABLE OrderExport (
  id BIGINT NOT NULL,
  amount DOUBLE
);
```

Learn more: https://docs.datasqrl.com/docs/sqrl-language/#create-table-internal-vs-external
