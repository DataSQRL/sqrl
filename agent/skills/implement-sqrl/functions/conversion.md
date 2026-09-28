# Conversion Functions

| SQL | Description |
|-----|-------------|
| `CAST(value AS type)` | Returns a new value being cast to type type. A CAST error throws an exception and fails the job. When performing a cast operation that may fail, like STRING to INT, one should rather use TRY_CAST, in order to handle errors. If "table.exec.legacy-cast-behaviour" is enabled, CAST behaves like TRY_CAST. E.g., CAST('42' AS INT) returns 42; CAST(NULL AS STRING) returns NULL of type STRING; CAST('non-number' AS INT) throws an exception and fails the job. |
| `TRY_CAST(value AS type)` | Like CAST, but in case of error, returns NULL rather than failing the job. E.g., TRY_CAST('42' AS INT) returns 42; TRY_CAST(NULL AS STRING) returns NULL of type STRING; TRY_CAST('non-number' AS INT) returns NULL of type INT; COALESCE(TRY_CAST('non-number' AS INT), 0) returns 0 of type INT. |
| `TYPEOF(input) TYPEOF(input, force_serializable) ` | Returns the string representation of the input expression's data type. By default, the returned string is a summary string that might omit certain details for readability. If force_serializable is set to TRUE, the string represents a full data type that could be persisted in a catalog. Note that especially anonymous, inline data types have no serializable string representation. In this case, NULL is returned. |
