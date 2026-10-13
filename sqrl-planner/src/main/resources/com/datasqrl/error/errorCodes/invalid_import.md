The `IMPORT` statement could not be parsed or resolved.

Imports reference another SQRL script by a `.` separated path relative to the current script (e.g. `connectors.sources` maps to `./connectors/sources.sqrl`). An `AS` alias must be a single name.

Erroneous code example:
```sql
IMPORT connectors.sources.Click AS events.Click;
```

Use one of the supported forms:
```sql
IMPORT connectors.sources.*;                    -- inline all statements of the script
IMPORT connectors.sources AS src;               -- tables available as src.Click
IMPORT connectors.sources.Click AS ClickEvents; -- a single table
```

When the importing script is in a subdirectory, namespaces configured under `script.include` in `package.json` need the `root` prefix, e.g. `IMPORT root.data_catalog.sources;`.
