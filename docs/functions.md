# Functions

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

| Function Name                        | Description                                                                                                                         |
|--------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `JsonExtract(json, String, Any)` | Extracts a value from a JSON object based on a JSON path specification. Returns the value at the specified path, or a default value if the path is not found or in case of an error. Useful for navigating complex JSON structures. |
| `ToJson(Any)`                        | Converts a given input into a `json` object. This conversion is useful for facilitating JSON manipulations within Flink data flows. Can handle inputs like strings representing JSON, other JSON objects, or various scalar values to create JSON structures. |
| `JsonToString(json)`        | Converts a `json` JSON object back to a string representation. Useful for outputting or exporting JSON data after transformations within Flink. |
| `JsonConcat(json, json)` | Merges two JSON objects, combining all key-value pairs from both. Useful for aggregating data from different JSON sources into a single JSON structure. |
| `JsonObject(Any...)`                 | Creates a JSON object from provided key-value pairs. Each pair consists of a string key and a value which can be any type, forming a JSON object dynamically. |
| `JsonArray(Any...)`                  | Constructs a JSON array from given elements of any type. This function allows for dynamic JSON array creation, accommodating a variety of data types. |
| `JsonQuery(json, String)`   | Executes a JSON path query on a JSON object and returns the query result as a JSON string. Ideal for extracting specific data from nested JSON structures. |
| `JsonExists(json, String)`  | Evaluates whether a specified JSON path exists within a JSON object, returning a boolean indicating presence or absence of the path. |
| `JsonArrayAgg(Any...)`               | Aggregates multiple values into a JSON array during a group by operation. Can handle various types of input to dynamically build a JSON array. |
| `JsonObjectAgg(String, Any...)`      | Similar to `JsonArrayAgg` but for JSON objects, aggregating key-value pairs into a JSON object during group operations, handling complex aggregation logic with JSON structures. |
| `JsonConcat(json...)`       | An extended function to concatenate multiple JSON objects into one, merging them by adding all key-value pairs from each JSON into a single object. |

### Text Functions

| Function Name           | Description                                                                                                                                                                                                                                                                                        |
|-------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Format(String, String...)` | A function that formats text based on a format string and variable number of arguments. It uses Java's `String.format` method internally. If the input text is `null`, it returns `null`. For example, `Format("Hello %s!", "World")` returns "Hello World!".                                        |
| `TextSearch(String, String...)` | Evaluates a query against multiple text fields and returns a score based on the frequency of query words in the texts. It tokenizes both the query and the texts, and scores based on the proportion of query words found in the text. For example, `TextSearch("hello", "hello world")` returns `1.0`. |

### Time Functions

| Function Name                        | Description                                                                                                                                                                                           | 
|--------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `AtZone(timestamp, zone)`            | Converts a timestamp to a different time zone specified by `zone`. For example, `AtZone(NOW(), 'America/New_York')` converts the current timestamp to Eastern Standard Time.                          |
| `EpochMilliToTimestamp(epochMillis)` | Converts milliseconds since the Unix epoch (January 1, 1970, 00:00:00 GMT) to a TIMESTAMP. For example, `EpochMilliToTimestamp(1609459200000L)` converts the given epoch milliseconds to a timestamp. |
| `EpochToTimestamp(epoch)`            | Converts seconds since the Unix epoch (January 1, 1970, 00:00:00 GMT) to a TIMESTAMP. For example, `EpochMilliToTimestamp(1609459200L)` converts the given epoch seconds to a timestamp.              |
| `ParseTimestamp(s, format)`          | Parses a timestamp from a given string using the specified format. For example, `ParseTimestamp('2021-01-01T12:00:00Z', 'yyyy-MM-dd\'T\'HH:mm:ssX')` returns the corresponding timestamp.             |
| `TimestampToEpoch(timestamp)`        | Converts a TIMESTAMP to epoch seconds. For example, `TimestampToEpoch(TO_TIMESTAMP('1970-01-01 00:00:00'))` returns 0.                                                                                |
| `TimestampToEpochMilli(timestamp)`   | Converts a TIMESTAMP to epoch milliseconds. For example, `TimestampToEpochMilli(TO_TIMESTAMP('1970-01-01 00:00:00'))` returns 0.                                                                      |
| `TimestampToString(timestamp)`       | Returns an ISO-8601 representation of the timestamp argument                                                                                                                                          |
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

Check out the [SQRL functions repository](https://github.com/DataSQRL/sqrl-functions/) for all standard libraries.

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