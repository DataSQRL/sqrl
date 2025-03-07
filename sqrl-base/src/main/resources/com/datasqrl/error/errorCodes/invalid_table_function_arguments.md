Invalid table function arguments provided.

Table function arguments are defined like columns in a CREATE TABLE statement: the argument name followed by the argument type with multiple arguments separated by a comma `,`.
Inside the query body, arguments are referenced by name prefixed with a colon `:`.

For example:
```
MyTableFunction(argument INT, arg2 STRING NOT NULL) := SELECT * FROM MyTable WHERE col1 = :arg2 AND col2 > :argument
```