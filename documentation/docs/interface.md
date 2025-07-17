# Designing the Interface

Based on the SQRL script, DataSQRL generates the interface for the compiled data pipeline. DataSQRL supports the following interfaces:

* GraphQL (Mutations, Queries, and Subscriptions)
* MCP (Tooling and Resources)
* REST (GET and POST)
* Data Product (Data Lake and Database Views)

The first 3 are APIs that can be invoked programmatically. The generated APIs interfaces are configured by the `enabled-apis` [compiler configuration](configuration.md#compiler-compiler).

For data products, DataSQRL generates view definitions as deployment assets in `build/deploy/plan` which can be queried directly.

## Data Products

For data products, each visible table defined in the SQRL script is exposed as a view or physical table depending on the pipeline optimization. The mapping between visible tables in the SQRL script and exposed tables in the interface is 1-to-1.

We recommend generating unique table names for the physical tables by configuring a table-name suffix in the [connector configuration](configuration.md). This separates views from physical tables to provide modularity and support updates without impacting downstream consumers. 

## APIs

### GraphQL

DataSQRL uses the GraphQL data model as the base model for all API access to data. The [SQRL language specification](sqrl-language.md#api-mapping) defines how the tables and relationships in the SQRL script map to GraphQL types and fields, as well as how tables map to queries, mutations, and subscriptions.

To generate the GraphQL schema from a SQRL script, add the `--api graphql` option to the [compile command](compiler.md#compile-command).

You can customize the GraphQL schema by:
* Changing field cardinalities (e.g. `[Person]!` to `Person!`)
* Changing scalar types (e.g. `Long` to `Int`)
* Changing the argument name for mutations (e.g. `event` to `payload`)
* Changing the type of fields to compatible types (e.g. `Person` to `SpecificPerson`)
* Adding enums
* Adding interfaces and structuring types with interfaces

Note, that changes must be compatible with the underlying data as defined in the SQRL script and the [API mapping](sqrl-language.md#api-mapping).

### MCP and REST

