The `EXPORT` statement could not be parsed or its sink path is invalid.

Erroneous code example:
```sql
EXPORT HighValueOrders;
```

An `EXPORT` statement names the table to export and the sink to write it to. The sink is a built-in sink (`print.*`, `logger.*`, `log.*`) or a table declared in another SQRL script:
```sql
EXPORT HighValueOrders TO print.HighValueOrders;
EXPORT HighValueOrders TO log.HighValueOrders;
EXPORT HighValueOrders TO connectors.sinks.OrderExport;
```

When the exporting script is in a subdirectory, sinks from a namespace configured under `script.include` in `package.json` need the `root` prefix, e.g. `EXPORT HighValueOrders TO root.data_catalog.sinks.OrderExport;`.
