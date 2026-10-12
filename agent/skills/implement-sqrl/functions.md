# Functions

Look up available functions for data processing that cannot be accomplished with SQL alone.
DataSQRL supports the following types of Flink functions: (async) scalar functions, aggregate functions, (async) table functions, and process table functions.

## System Functions

DataSQRL supports all of Flink SQL built-in system functions and adds additional system functions.
System functions can be used without import. 

**Flink SQL Built-in Functions:**
* [aggregate](functions/aggregate.md): Aggregation functions like COUNT, SUM, AVG, MIN, MAX, COLLECT, LISTAGG, and window aggregates.
* [arithmetic](functions/arithmetic.md): Numeric operations including basic math (+, -, *, /), rounding, trigonometry, and logarithms.
* [collection](functions/collection.md): Array and map operations like CARDINALITY, ELEMENT, ARRAY_CONTAINS, MAP_KEYS, and ARRAY_AGG.
* [comparison](functions/comparison.md): Comparison operators (=, <>, <, >), NULL checks, BETWEEN, LIKE, SIMILAR TO, and IN.
* [conditional](functions/conditional.md): CASE expressions, NULLIF, COALESCE, IF, IFNULL, and NVL functions.
* [conversion](functions/conversion.md): Type casting with CAST, TRY_CAST, and TYPEOF.
* [hashfunctions](functions/hashfunctions.md): Hash functions including MD5, SHA1, SHA224, SHA256, SHA384, and SHA512.
* [json](functions/json.md): JSON parsing, construction, querying with JSON_VALUE, JSON_QUERY, JSON_OBJECT, and JSON_ARRAY.
* [logical](functions/logical.md): Boolean operators AND, OR, NOT, and IS TRUE/FALSE/UNKNOWN checks.
* [string](functions/string.md): String manipulation including concatenation, SUBSTRING, TRIM, UPPER, LOWER, REGEXP, and SPLIT.
* [temporal](functions/temporal.md): Date/time functions for parsing, formatting, extraction, arithmetic, and timezone handling.
* [variant](functions/variant.md): VARIANT type functions for parsing and handling semi-structured data.

**SQRL System Functions:**
* [jsonb](functions/jsonb.md): Binary JSON functions for efficient semi-structured data handling (to_jsonb, jsonb_extract, jsonb_object, jsonb_array).
* [vector](functions/vector.md): Vector operations for embeddings including cosine_similarity, euclidean_distance, and vector conversion.
* [text](functions/text.md): Text formatting, splitting, and full-text search functions.


## Function Libraries

SQRL includes standard libraries that can be imported into a SQRL script as follows:

```sql
IMPORT stdlib.math.*;
```
Imports all functions from the `math` library into the script. Replace `math` with the library you wish to import.

```sql
IMPORT stdlib.math.hypot AS hypotenuse;
```
Imports a single function `hypot` from the `math` library under the name `hypotenuse`. The renaming with `AS` is optional and is omitted when you want to use the original name.

DataSQRL provides the following libraries:

* [math](functions/math.md): Extended math functions including cbrt, hypot, statistical distributions (binomial, exponential, normal, poisson).
* [openai](functions/openai.md): OpenAI API integration for completions, JSON extraction, and vector embeddings for any OpenAI compatible LLM provider. Requires `OPENAI_API_KEY` environment variable.
* [iceberg](functions/iceberg.md): Apache Iceberg table utilities for partition analysis and duplicate data management.

## User Defined Functions

If the required functionality cannot be achieved with available system or library functions, use the `/implement-udf` skill to implement a user defined function.
