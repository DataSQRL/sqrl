# Designing the Interface

Based on the SQRL script, DataSQRL generates the interface for the compiled data pipeline. DataSQRL supports the following interfaces:

* GraphQL (Mutations, Queries, and Subscriptions)
* MCP (Tooling and Resources)
* REST (GET and POST)
* Data Product (Data Lake and Database Views)

The first three are APIs that can be invoked programmatically. These generated API interfaces are configured by the `protocols` [compiler configuration](configuration.md#compiler-compiler).

For data products, DataSQRL generates view definitions as deployment assets in `build/deploy/plan` which can be queried directly.

## Data Products

For data products, each visible table defined in the SQRL script is exposed as a view or physical table depending on the pipeline optimization. The mapping between visible tables in the SQRL script and exposed tables in the interface is 1-to-1.

We recommend generating unique table names for the physical tables by configuring a table-name suffix in the [connector configuration](configuration#connectors-connectors), e.g. by configuring the `table-name` for `postgres` or the `catalog-name` for `iceberg` to `${sqrl:table-name}_MY_SUFFIX` . This separates views from physical tables to provide modularity and support updates without impacting downstream consumers. 

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

:::warning
Changes must be compatible with the underlying data as defined in the SQRL script and the [API mapping](sqrl-language.md#api-mapping).
:::

To use a customized GraphQL schema, configure the schema explicitly in the [script configuration](configuration.md#script-script) or by providing it as a [command argument](compiler.md).

### MCP and REST

DataSQRL generates a list of operations from the GraphQL schema: one for each query and mutation endpoint. 
Queries are mapped to MCP tools with a `Get` prefix and REST endpoints under `rest/queries`. If the arguments are simple scalars, the REST endpoint is GET with URL parameters, otherwise POST with the arguments as payload.
Mutations are mapped to MCP tools with an `Add` prefix and REST POST endpoints under `rest/mutations`.
For the result set, DataSQRL follows relationship fields up to a configured depth (and without loops).

You can control the default operation generation with the [compiler configuration](configuration.md#compiler-compiler).

For complete control over the exposed MCP tools and resources as well as REST endpoints, you can define the operations explicitly in a separate GraphQL file (or multiple files).

The GraphQL file defining the operations contains one or multiple query or mutation queries.
The name of the operation is the name of the MCP tool and REST endpoint.

The `@api` directive controls how the operation is exposed:
* `rest`: `NONE`, `GET`, or `POST` to configure the HTTP method or not expose as REST endpoint.
* `mcp`: `NONE`, `TOOL`, or `RESOURCE` to configure how the query is exposed in MCP.
* `uri`: AN RFC 6570 template to configure the REST path and MCP resource path. Any operation arguments that are not defined in the uri template are considered part of the payload for REST (and the method must be POST).

```graphql
query GetPersonByAge($age: Int!) @api(rest: GET, mcp: TOOL, uri: "/queries/personByAge/{age}") {
    Person(age: $age, limit: 10, offset: 0) {
        name
        email
    }
}
```

This defines an operation `GetPersonByAge` which is the name of the MCP tool and REST endpoint with the path `/queries/personByAge/{age}` using GET method.