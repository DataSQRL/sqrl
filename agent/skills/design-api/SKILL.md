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
- A relationship field resolves to `<GraphQL type name>.<field name>` or, if there is neither such a relationship nor a column with that name, to the relationship on the base table. All tables/functions that return a type must resolve its relationship fields the same way (see [interface.md](interface.md)).

## REST and MCP Endpoints

**Auto-generated from GraphQL:**
- Queries → MCP tools with `Get` prefix, REST GET endpoints under `rest/queries`
- Mutations → MCP tools with `Add` prefix, REST POST endpoints under `rest/mutations` (nested inputs become nested JSON tool arguments)
- `@api(mcp: NONE)` on a query or mutation field omits its generated operation from MCP
- Results follow relationships in GraphQL up to configured `max-result-depth`
- Mutation input types are followed to the leaf input types to construct the expected request payload. 
- MCP is served over Streamable HTTP at `/<version>/mcp` (e.g. `/v1/mcp`) and supports MCP protocol versions `2024-11-05`, `2025-03-26`, `2025-06-18`, and `2025-11-25`. The server echoes the client's requested version if supported, otherwise it responds with `2025-11-25` (see [interface.md](interface.md)).

### Custom Operations

Defined in `.graphql` files and configured in `package.json` under `script.operations`, or under `script.api.<version>.operations` for a versioned API.

```graphql
# Returns up to 10 people of the given age.
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
- `compiler.api.endpoints: OPS_ONLY` to expose only custom operations and restrict a public GraphQL endpoint to those stored named operations. `GRAPHQL` keeps public GraphQL flexible while exposing only explicit REST/MCP operations; `FULL` is the default and adds generated operations.

## API Descriptions

API consumers read these descriptions in GraphQL, REST (OpenAPI) and MCP. Each one comes from a single source:

| API part | Source of the description |
|---|---|
| Generated schema | The SQRL `/** ... */` doc-strings, with their `# Columns:` and `# Arguments:` sections (see `/implement-sqrl`, Comments & Doc-strings) |
| Custom schema | The `"..."` or `"""..."""` descriptions in the schema file. A `#` line above a type, field, argument, input field or enum value without a `"..."` description also becomes its description. The SQRL doc-strings do not reach the API |
| Custom operation | All `#` lines above the operation, back to the end of the previous operation or to the top of the file, joined with spaces into one text. So a `#` note between two operations, or in the file header, becomes part of the next operation's description. They describe the REST operation and the MCP tool or resource. A `#` line above a variable describes that argument in MCP, and in the REST request body of a `POST` operation. A `"""` description before an operation fails the compile with `Invalid syntax` |

Write every description for the API consumer. Describe each field, argument, input field and enum value whose name does not show its full meaning. For a mutation field in a custom schema, state what it creates or changes and what it returns.

Example from a custom schema:

```graphql
"The status of an event or action item."
enum UpdateStatus {
  "Not started or in progress. The default when no update has set a status."
  open
  "Completed."
  done
}

type Query {
  "One event by its key, or null when it does not exist or the caller cannot see it."
  Event(
    "Subject identifier of the event's author."
    author_sub: String!,
    "Business time of the event in epoch seconds."
    event_time: Long!
  ): EventData
}

type Mutation {
  """
  Creates an event, or replaces the existing event with the same key, and returns the stored event.
  """
  upsertEvent(event: upsertEventInput!): upsertEventResultOutput!
}
```

Put maintainer notes where DataSQRL does not publish them. In an operations file, write them as `#` lines inside the operation body. For a custom schema, write them as `--` comments in the SQRL script.

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

DataSQRL authenticates JWT tokens as configured in the server engine and maps claims to function arguments and mutation columns with `METADATA FROM 'auth.[claim-name]'`.
Use the `/implement-sqrl` skill for the SQRL rules of each claim: where a claim can go, its type, and required and optional claims.

```sql
MyUser(customerId BIGINT NOT NULL METADATA FROM 'auth.userId') :=
  SELECT c.* FROM Customer c WHERE c.customerId = :customerId LIMIT 1;
```

Maps to query endpoint `MyUser: Customer`; `customerId` is populated from the `userId` claim and is not a caller-supplied API argument.

Read [authorization configuration](authorization.md) to configure OAuth or JWT based authentication and authorization.

With an auth provider, every request needs a valid token, also for a query that reads no claim. So no query of the API can be public.

#### Record the caller of a mutation

Take the owner of a mutation record from the token. Declare the owner column on the mutation table with `METADATA FROM 'auth.<claim>'`. The column is then not part of the mutation input, so a caller cannot write a record for somebody else.
Add `no_query` to the mutation table, and give callers a claim function that returns their own records. Without `no_query`, the table gets its own query endpoint, and every caller reads every record. The mutation stays.

```sql
/*+no_query, engine(kafka) */
CREATE TABLE Reviews (
  review_id STRING NOT NULL METADATA FROM 'uuid',
  author_id STRING NOT NULL METADATA FROM 'auth.sub',
  product_id STRING NOT NULL,
  rating INT NOT NULL,
  created_at TIMESTAMP_LTZ(3) NOT NULL METADATA FROM 'timestamp'
);

/** The reviews of the caller. */
MyReviews(author_id STRING NOT NULL METADATA FROM 'auth.sub') :=
  SELECT * FROM Reviews WHERE author_id = :author_id;
```

#### Claims in a custom GraphQL schema

In a hand-written schema, follow the claim columns and arguments of the SQRL script:

- Leave each claim column out of the mutation input type.
- Add the claim column to the mutation result type when the caller needs it.
- Leave each claim argument out of the query field.

```graphql
type Mutation {
  Reviews(event: ReviewInput!): Review!
}

input ReviewInput {
  product_id: String!
  rating: Int!
}

type Review {
  review_id: String!
  author_id: String!
  product_id: String!
  rating: Int!
}

type Query {
  MyReviews(limit: Int = 10, offset: Int = 0): [Review!]
}
```

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
