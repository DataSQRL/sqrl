---
name: design-api
description: Use when designing or modifying the API exposed by a DataSQRL data pipeline. Use for GraphQL, REST, or MCP endpoints, access control, or data serving requirements. Use for adjusting exposed GraphQL schema.
---

Prefer designing the API implicitly by defining tables, functions, and relationships in SQRL that map to the desired API. After compiling, the generated API schema is found in `build/inferred_schema.graphqls`.
Only configure an explicit schema in the `package.json` configuration if:
- the schema needs to be static (e.g. to align with consumers of the API)
- the user wants to adjust the schema (e.g. introduce enums, add interfaces, remove fields, or rename/consolidate types)

Use the `/configure-sqrl` skill to configure the API, which protocols to expose, and how the exposed API endpoints are generated.

## Key Concepts

DataSQRL generates APIs from SQRL scripts with support for **GraphQL**, **REST**, **MCP**, and **Data Products**.

**Critical:** GraphQL is the authoritative model. All API requests (REST, MCP) are executed by the GraphQL query engine.

## SQRL to GraphQL Mapping

**Tables/Functions → Endpoints:**
- Default: Query endpoints (name and arguments match)
- With `SUBSCRIBE`: Subscription endpoints. **Before compiling any SUBSCRIBE statement, add `kafka` to `enabled-engines` in ALL package.json files.** Omitting kafka produces BUILD SUCCESS but the Subscription type will be silently absent from the generated schema.
- `CREATE TABLE` with `+engine` hint: Mutations (input excludes computed/metadata columns)

**IMPORTANT**: Only tables defined in the main script (or imported inline with `IMPORT otherscript.*`) are exposed in the API. 

**Schema Generation:**
- Tables → GraphQL types (columns → fields, nested rows → child objects)
- Relationships → relationship fields on types
- Compiler infers base tables to avoid redundant result types
- Hidden columns (starting with `_`) excluded from result types

**Example:**
```sql
Customer := SELECT customerId, email, name FROM RawCustomer WHERE email IS NOT NULL;
```
Maps to GraphQL query `Customer(limit: Int = 10, offset: Int = 0): [Customer!]` with type containing the three fields. Every query that returns multiple rows gets `limit`/`offset` pagination arguments; the `limit` default is the table's own `LIMIT` if it has one, otherwise `compiler.api.default-limit` (10).

**Schema Customization (preserving object-relationship mapping):**
- Change cardinalities, scalar types, mutation argument names, field types
- Add enums and interfaces
- Compiler validates compatibility with SQRL script(s)

## REST and MCP Endpoints

**Auto-generated from GraphQL:**
- Queries → eligible MCP tools with `Get` prefix, REST GET endpoints under `rest/queries`
- Mutations → eligible MCP tools with `Add` prefix, REST POST endpoints under `rest/mutations`
- Results follow relationships in GraphQL up to configured `max-result-depth`
- Mutation input types are followed to the leaf input types to construct the expected request payload. 

### Custom Operations

Defined in `.graphql` files and configured in `package.json` under `script.operations`, or under `script.api.<version>.operations` for a versioned API.

```graphql
""" Returns up to 10 people for a given age """
query GetPersonByAge($age: Int!)
  @api(rest: GET, mcp: TOOL, uri: "/queries/personByAge/{age}") {
  Person(age: $age, limit: 10, offset: 0) { name email }
}
```

**`@api` Directive:**
- `rest`: `NONE` | `GET` | `POST` (HTTP method or disable)
- `mcp`: `NONE` | `TOOL` | `RESOURCE` (MCP exposure type)
- `uri`: RFC 6570 template (args in template = path params, others = payload requiring POST)

**Rules:**
- Operation name = MCP tool name = REST endpoint name (must be unique)
- Doc strings propagate to API documentation
- `compiler.api.endpoints: OPS_ONLY` to expose only custom operations and restrict a public GraphQL endpoint to those stored named operations. `GRAPHQL` keeps public GraphQL flexible while exposing only explicit REST/MCP operations; `FULL` is the default and adds generated operations.

## API Entrypoints

To specify how the tables defined in the SQRL script are accessed you either use access hints or explicit table functions.

### Table Hints for Table Access

The `query_by_*` hints add the listed columns as arguments to the table query endpoint.

- `/*+query_by_all(col, ...)*/`: Required filter arguments: the argument must be supplied in every API call. Use it only when that column is always required to retrieve the data.
- `/*+query_by_any(col, ...)*/`: Optional filter arguments. Use it when requirements say "also by X", "queryable by X", or "filter by X".
- `/*+no_query*/`: no query endpoint of its own. The table stays in the API and is reached through a relationship or an explicit table function
- `_` prefix: not in the API at all (pipeline intermediate). 

Note: Never use `_` prefix with `no_query` as they are used for different purposes. See the `/implement-sqrl` skill (SQRL Language Spec, Interfaces) for the distinction.

### Explicit Table Function

Table functions are exposed as separate query endpoints in the API.

```sql
CustomerByIdRange(fromId BIGINT NOT NULL, toId BIGINT NOT NULL) := SELECT * FROM Customer WHERE customerId >= :fromId AND customerId < :toId ORDER BY customerId;
```

This definition maps to query endpoint `CustomerByIdRange(fromId: Long!, toId: Long!, limit: Int = 10, offset: Int = 0): [Customer!]`.

### Authentication & Authorization

DataSQRL authenticates JWT tokens as configured in the server engine and maps claims to function arguments with `METADATA FROM 'auth.[claim-name]'`.

```sql
MyUser(customerId BIGINT NOT NULL METADATA FROM 'auth.userId') :=
  SELECT c.* FROM Customer c WHERE c.customerId = :customerId LIMIT 1;
```

Maps to query endpoint `MyUser: Customer`; `customerId` is populated from the `userId` claim and is not a caller-supplied API argument.

Read [authorization configuration](authorization.md) to configure OAuth or JWT based authentication and authorization.

## API Versioning and OpenAPI

Top-level `script.graphql` and `script.operations` configure one `v1` API.
Use `script.api` to serve multiple versions concurrently; version names are `v` followed by a number.
A version can configure a schema, operations, or an OpenAPI compatibility baseline; SQRL infers the schema when one is not configured.
When `script.api` is present, it defines the API versions and the top-level API fields do not apply.

SQRL generates OpenAPI from the REST-exposed GraphQL operations for every compiled API version.
It is a generated description, not a second API definition.
Configure `script.api.<version>.openapi` with a prior generated document only to have compilation reject backwards-incompatible REST changes.

## Testing

Use the `/test-sqrl` skill to implement tests against the API.

## More Information

Review the [reference documentation](interface.md) for additional details:

* how SQRL table/function/relationship definitions map to GraphQL
* how REST/MCP endpoints are derived from GraphQL
* how to define explicit operations to explicitly defined REST/MCP API
* how API requests are executed
