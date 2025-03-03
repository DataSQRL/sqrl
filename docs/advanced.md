# Advanced DataSQRL Documentation

This page documents the advanced features of DataSQRL and extends the other documentation pages.

## Script Imports

SQRL supports importing table and function definitions from other SQRL scripts.

### Inline Script Imports

Inline imports place table and function definitions from another script into the current scope and requires
that table and function names do not clash with those in the importing script.

```sql
IMPORT myscript.*;
```
This statement imports all tables and functions from a SQRL script called `myscript.sqrl` in the local folder.

<!--
## Repository Imports

SQRL supports importing from remote repositories like GitHub.
To define such imports, the import path is prefixed with the repository URL.

```sql
IMPORT github.com/DataSQRL/sqrl-functions:sqrl-functions.openai.vector_embedding;
```
The statement above imports the `vector_embedding` function from the sqrl-functions repository.

Repository dependencies can also be defined in the [dependency section of the configuration](configuration.md#dependencies)
which is more convenient for multiple imports from the same repository and supports tags for versioning.

-->

## Data Discovery

DataSQRL automatically generates table definitions with connector configuration and schemas for json-line files (with extension `.jsonl`) and csv files (with extension `.csv`) within the project directory. This makes it easy to import data from such files into a SQRL project.

For example, to import data from a file `orders.jsonl` in the folder `mydata` you write:
```sql
IMPORT mydata.orders;
```

When you run the compiler, it will create the table configuration file `orders.table.sql` and analyze the data to extract the schema
in yml format in the `orders.schema.yml` file. Those are then imported.

To disable automatic discovery of data for a directory, place a file called `.nodiscovery` into that directory.



