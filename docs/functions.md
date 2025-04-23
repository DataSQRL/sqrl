# Functions

SQRL adds additional functions to the standard SQL function catalog.

Functions in SQRL are designed to be engine-agnostic, ensuring that their implementation is consistent across different platforms and execution environments. This uniformity is crucial for maintaining the semantic integrity of functions when executed under various systems.

**Characteristics of Functions**
- **Engine Agnosticism**: Functions are defined in a way that does not depend on the specifics of the underlying engine.
- **Semantic Consistency**: Regardless of the engine used, function should preserve their semantic meaning.
- **Mixed Engine Support**: While functions are designed to be widely supported, some may have mixed support depending on the engine's capabilities.
- **Nullability Awareness**: Functions in SQRL retain nullability information. This feature is vital for correct schema generation downstream, ensuring that data integrity is maintained through the potential propagation of null values.


:::warn
This documentation is work-in-progress. SQRL does not yet support function mappings for all engines. Stay tuned.
:::

## Flink System Functions

SQRL supports all [FlinkSQL built-in functions](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/functions/systemfunctions/) which covers most basic functions.

## SQRL System Functions

### Common Functions

| Function Name                        | Description                                                                                                                                                                                           | 
|--------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SerializeToBytes(object)`           | Serializes the given object to a byte array                                                                                                                                                           |


### JSON Functions

These functions should be used instead of the ones provided by Flink since they use the native JsonType that SQRL adds to the type system.

| Function Name       | Description                                                                                          | Example Usage                                                                                     |
|---------------------|------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| `toJson`            | Parses a JSON string or Flink object (e.g., `Row`, `Row[]`) into a JSON object.                      | `toJson('{"name":"Alice"}')` → JSON object                                                        |
| `jsonToString`      | Serializes a JSON object into a JSON string.                                                         | `jsonToString(toJson('{"a":1}'))` → `'{"a":1}'`                                                    |
| `jsonObject`        | Constructs a JSON object from key-value pairs. Keys must be strings.                                 | `jsonObject('a', 1, 'b', 2)` → `{"a":1,"b":2}`                                                     |
| `jsonArray`         | Constructs a JSON array from multiple values or JSON objects.                                        | `jsonArray(1, 'a', toJson('{"b":2}'))` → `[1,"a",{"b":2}]`                                         |
| `jsonExtract`       | Extracts a value from a JSON object using a JSONPath expression. Optionally specify default value.   | `jsonExtract(toJson('{"a":1}'), '$.a')` → `1`                                                      |
| `jsonQuery`         | Executes a JSONPath query on a JSON object and returns the result as a JSON string.                  | `jsonQuery(toJson('{"a":[1,2]}'), '$.a')` → `'[1,2]'`                                              |
| `jsonExists`        | Returns `TRUE` if a JSONPath exists within a JSON object.                                            | `jsonExists(toJson('{"a":1}'), '$.a')` → `TRUE`                                                    |
| `jsonConcat`        | Merges two JSON objects. If keys overlap, the second object's values are used.                       | `jsonConcat(toJson('{"a":1}'), toJson('{"b":2}'))` → `{"a":1,"b":2}`                               |
| `jsonArrayAgg`      | Aggregate function: accumulates values into a JSON array.                                            | `SELECT jsonArrayAgg(col) FROM tbl`                                                               |
| `jsonObjectAgg`     | Aggregate function: accumulates key-value pairs into a JSON object.                                  | `SELECT jsonObjectAgg(key_col, val_col) FROM tbl`                                                 |

### Text Functions

| Function Name                    | Description                                                                                                                                                                                                                                                                                              |
|----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `format(String, String...)`      | A function that formats text based on a format string and variable number of arguments. It uses Java's `String.format` method internally. If the input text is `null`, it returns `null`. For example, `format("Hello %s!", "World")` returns "Hello World!".                                            |
| `text_search(String, String...)` | Evaluates a query against multiple text fields and returns a score based on the frequency of query words in the texts. It tokenizes both the query and the texts, and scores based on the proportion of query words found in the text. For example, `text_search("hello", "hello world")` returns `1.0`. |

### Vector Functions

| Function Name                                                         | Description                                                                                                                                                                                                                                                                                        |
|-----------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `CosineSimilarity(FlinkVectorType vectorA, FlinkVectorType vectorB)`  | Calculates the cosine similarity between two vectors. This is a common operation in many vector space models in machine learning, especially useful in text analytics to find the similarity between documents. Example: `cosineSimilarity(vector1, vector2)` returns the cosine similarity score. |
| `CosineDistance(FlinkVectorType vectorA, FlinkVectorType vectorB)`    | Computes the cosine distance between two vectors, which is `1 - cosineSimilarity`. This function is used to measure how different two vectors are. Example: `cosineDistance(vector1, vector2)` gives a measure of the distance between two vectors.                                                |
| `EuclideanDistance(FlinkVectorType vectorA, FlinkVectorType vectorB)` | Calculates the Euclidean distance between two vectors, a direct measure of the distance in the vector space. Example: `euclideanDistance(vector1, vector2)` computes the distance, useful in clustering and other machine learning algorithms that rely on distance calculations.                  |
| `Center(vector)`                                                      | Used as an aggregate function to compute the center (mean vector) of a group of vectors during aggregation operations in Flink. This function is typically used in clustering or when calculating the centroid of data points.                                                                     |
| `DoubleToVector(double array)`                                        | Converts a double array to a vector                                                                                                                                                                                                                                                                | 
| `VectorToDouble(vector)`                                              | Converts a vector to a double array                                                                                                                                                                                                                                                                | 


## Additional Function Libraries

The following function libraries can be imported into SQRL scripts:

* [Math](https://github.com/DataSQRL/sqrl-functions/tree/main/sqrl-math): Contains a number of advanced math functions beyond the basic SQL arithmetic functions in the system library.
* [OpenAI](https://github.com/DataSQRL/sqrl-functions/tree/main/sqrl-openai): Functions that invoke OpenAI for vector embeddings, completions, and structured data extraction.

Check out the [SQRL functions repository](https://github.com/DataSQRL/sqrl-functions/) for additional libraries that can be imported into SQRL scripts.

## User Defined Functions

Users can define custom functions and import them into a SQRL script. 

### Java

To create a new function package, first create a sub-folder `myjavafunction` in the directory of the script where you want to import the functions.
Inside that package, create a java project which implements a [Flink function](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/functions/udfs/).
Annotate the function with `@AutoService` and the Flink function that it extends so the function can be discovered by the compiler.

Compile the java project into a jar file and import it into the SQRL script via:
```sql
IMPORT myjavafunction.target.MyScalarFunction;
```

Check out this [complete example](https://github.com/DataSQRL/datasqrl-examples/tree/main/user-defined-function).


### JavaScript

Support for JavaScript functions is currently being implemented and is targeted for the 0.7 release.

### Python

Support for Python functions is currently being implemented and is targeted for the 0.7 release.