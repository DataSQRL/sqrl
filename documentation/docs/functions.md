# Functions

## System Functions

DataSQRL supports all of [Flink's built-in system functions](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/functions/systemfunctions/).

SQRL adds [system functions](functions-system-generated) with support for:
* a binary JSON type (JSONB) to represent semi-structured data efficiently.
* a vector type to represent embeddings.
* text manipulation and full text search.

System functions are always available and do not need to be imported. Take a look at the [full list of SQRL system function](functions-system-generated).

## Function Libraries

SQRL includes [standard libraries](functions-library-generated) that can be imported into a SQRL script as follows:

```sql
IMPORT stdlib.math;
```
Imports all functions from the `math` library into the script. Replace `math` with the library you wish to import.

```sql
IMPORT stdlib.math.hypot AS hypotenuse;
```
Imports a single function `hypot` from the `math` library under the name `hypotenuse`. The renaming with `AS` is optional and is omitted when you want to use the original name.

Check out the [full list of function libraries](functions-library-generated).

## User Defined Functions

Extend SQRL with Java implementations of [Flink user-defined functions (UDFs)](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/table/functions/udfs/), such as scalar, table, aggregate, and asynchronous scalar functions.
SQRL discovers functions in your project and makes them available through `IMPORT` statements.

Choose the authoring workflow that fits your function:

* [JBang scripts](#jbang-scripts) are the quickest option for self-contained functions or functions with a small number of dependencies. DataSQRL builds them during compilation.
* [Java projects](#java-projects) are appropriate for larger UDFs, shared source code, extensive testing, or an existing Maven or Gradle build. The JAR must be built and packaged manually!

In both workflows, the function is imported by its project directory and class name, not by its Java package name. For example, a function placed in `usrlib/` is imported as:

```sql
IMPORT usrlib.MyScalarFunction;
```

### JBang Scripts

[JBang](https://www.jbang.dev/) lets you write a UDF as a single Java source file. DataSQRL builds and packages these scripts during compilation. Installing JBang locally is optional, it can be useful to build a script independently:

```shell
jbang --version
```

Place the script in a directory within the SQRL project. The following layout defines a function named `MyScalarFunction` that can be imported from `myudf.sqrl`:

```text
my-project/
├── myudf.sqrl
└── usrlib/
    └── MyScalarFunction.java
```

Every JBang UDF script must meet these requirements:

* Its first line must be exactly `///usr/bin/env jbang "$0" "$@" ; exit $?`.
* It must contain exactly one `public` class that extends a supported Flink UDF base class, such as `ScalarFunction`, `TableFunction`, or `AggregateFunction`.
* The public class name must match the file name.

For example, create `usrlib/MyScalarFunction.java`:

```java
///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS org.apache.flink:flink-table-common:2.3.0

import org.apache.flink.table.functions.ScalarFunction;

public class MyScalarFunction extends ScalarFunction {

  public long eval(long left, long right) {
    return left + right;
  }
}
```

Import and use the function in your SQRL script:

```sql
IMPORT usrlib.MyScalarFunction;

Result := SELECT MyScalarFunction(2, 3) AS sum;
```

#### Dependencies

Declare dependencies with JBang's `//DEPS` directives. At the moment JBang does not support provided dependencies, so every JBang UDF must declare `flink-table-common` explicitly.
Use the Flink version supported by the DataSQRL release:

```java
///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS org.apache.flink:flink-table-common:2.3.0
//DEPS org.apache.commons:commons-text:1.12.0

import org.apache.commons.text.WordUtils;
import org.apache.flink.table.functions.ScalarFunction;

public class TitleCase extends ScalarFunction {

  public String eval(String value) {
    return WordUtils.capitalizeFully(value);
  }
}
```

DataSQRL compiles all eligible JBang scripts in the project into a deployable JAR and discovers their functions automatically. No `@AutoService` annotation or manual JAR build is required. It caches the result as `jbang-udfs.jar` in the UDF script directory and rebuilds it when a script changes. Delete `jbang-udfs.jar` to force a rebuild.

### Java Projects

Use a standard Java project when your UDF has a more involved build, shared code, or dependencies that are better managed with Maven or Gradle. Place the built JAR in a directory within the SQRL project. The directory containing the JAR becomes the import namespace.

For example, this project layout imports `MyScalarFunction` from the Maven `target` directory:

```text
my-project/
├── myudf.sqrl
└── myjavafunction/
    ├── pom.xml
    ├── src/
    │   └── main/java/com/example/MyScalarFunction.java
    └── target/
        └── myjavafunction.jar
```

Implement a Flink UDF and register it with Java's service loader. The [`@AutoService`](https://github.com/google/auto/tree/main/service) annotation generates the required registration when your build includes its annotation processor:

```java
package com.example;

import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.ScalarFunction;

@AutoService(ScalarFunction.class)
public class MyScalarFunction extends ScalarFunction {

  public long eval(long left, long right) {
    return left + right;
  }
}
```

If you do not use `@AutoService`, add the equivalent service-loader file yourself. For the example above, create `META-INF/services/org.apache.flink.table.functions.ScalarFunction` in the JAR with this content:

```text
com.example.MyScalarFunction
```

Package any dependencies your UDF needs at runtime into the deployable JAR. Mark Flink dependencies as provided or compile-only so that the UDF uses DataSQRL's Flink runtime rather than shipping a second copy.

Build the project before compiling SQRL, then import the function using the directory that contains the JAR:

```shell
mvn -f myjavafunction/pom.xml package
```

```sql
IMPORT myjavafunction.target.MyScalarFunction;
```

DataSQRL scans JARs under the project directory during compilation, discovers registered Flink UDFs, and packages the JAR with the application.

See the [complete UDF examples](https://github.com/DataSQRL/datasqrl-examples/tree/main/user-defined-function), including [JBang](https://github.com/DataSQRL/datasqrl-examples/tree/main/user-defined-function/jbang) and [Maven project](https://github.com/DataSQRL/datasqrl-examples/tree/main/user-defined-function/maven-project) variants.
