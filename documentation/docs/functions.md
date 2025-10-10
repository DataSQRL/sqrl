# Functions

## System Functions

SQRL supports all of [Flink's built-in system functions](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/functions/systemfunctions/).

SQRL adds [system functions](functions-system) with support for:
* a binary JSON type (JSONB) to represent semi-structured data efficiently.
* a vector type to represent embeddings.
* text manipulation and full text search.

System functions are always available and do not need to be imported. Take a look at the [full list of SQRL system function](functions-system).

## Function Libraries

SQRL includes [standard libraries](functions-library) that can be imported into a SQRL script as follows:

```sql
IMPORT stdlib.math;
```
Imports all functions from the `math` library into the script. Replace `math` with the library you wish to import.

```sql
IMPORT stdlib.math.hypot AS hypotenuse;
```
Imports a single function `hypot` from the `math` library under the name `hypotenuse`. The renaming with `AS` is optional and is omitted when you want to use the original name.

Check out the [full list of function libraries](functions-library).


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