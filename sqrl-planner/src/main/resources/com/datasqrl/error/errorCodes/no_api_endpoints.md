The pipeline includes a server engine, but the script does not define any table or function that can be queried through the API.

Tables and functions whose name starts with `_` are hidden, and exports or mutations alone do not define an API.

Erroneous code example:
```sql
IMPORT connectors.sources.*;

_HighValueOrders := SELECT * FROM Orders WHERE amount > 100;
EXPORT _HighValueOrders TO log.HighValueOrders;
```

Define at least one visible table or function:
```sql
IMPORT connectors.sources.*;

HighValueOrders := SELECT * FROM Orders WHERE amount > 100;
```

If the pipeline does not need an API, remove the server engine (e.g. `vertx`) from `enabled-engines` in `package.json`.
