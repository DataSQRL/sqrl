---
name: test-sqrl
description: Use when adding or updating tests for a DataSQRL pipeline. Use after implementing features to verify correctness.
---

Always implement test coverage for major features or complex data transformations implemented in SQRL.
Prefer table tests. Use API tests for complex interaction patterns (mutation followed by query/subscription) or authorization.

# Writing Tests

## Test Types

DataSQRL supports two snapshot test types (run via `test` command):
1. **Table tests**: SQRL tables annotated with `/*+test */` hint
2. **API tests**: GraphQL operations (queries/mutations/subscriptions) in a configured test folder.

## Table Tests

Add `/*+ test */` hint to any table definition:

```sql
-- Tests that the query returns an identical snapshot of data
/*+ test */
CustomerRetrievalTest :=
SELECT customerid, name, email
  FROM Customer
  WHERE name = 'John' ORDER BY customerid;

-- Tests an invariant and returns failing records
/*+ test(no_rows) */
InvalidCustomersTest :=
SELECT * FROM Customer
  WHERE name = '' OR email IS NULL;
```

**Requirements:**
- Use `/*+ test(no_rows) */` hint to test invariants
- Snapshot results must have deterministic order (use `ORDER BY`)
- Only select predictable columns and exclude non-deterministic ones (e.g. columns computed via `RAND()` or `NOW()`)
- Table tests do not include relationships
- Every column in `ORDER BY` must also appear in `SELECT`. To sort by a column without including it in snapshot output, alias it with a `_` prefix: `event_time AS _event_time`. Omitting an ORDER BY column from SELECT causes: `All sort columns must be part of the SELECT clause for table definitions, missing: [col]`.

## API Tests

Place `.graphql` files in the configured test folder, **one operation definition per file**. The file name (without extension) is the test name and the snapshot name: every `query`/`mutation`/`subscription` block in a file is registered under that name, so a second block in the same file is compared against the first one's snapshot and fails on every run. One block may still query several root fields at once (use aliases: `byOrg: alerts(...) {...}  byDeployment: alerts(...) {...}`); that is a single operation and a single snapshot.

`GetAllCustomers.graphql`:
```graphql
query GetAllCustomers {
  Customer { customerid name email }
}
```

`AddCustomer.graphql`:
```graphql
mutation AddCustomer {
  Customer(event: {customerid: 123, email: "test@example.com", name: "Test"}) {
    customerid
  }
}
```

**Test Execution Order:**
1. Subscribe to all subscriptions (must be triggered by mutation)
2. Execute mutations sequentially (alphabetical filename order) with `mutation-delay-sec` between
3. Wait for configured timeout, then drain the Flink job
4. Execute queries
5. Collect subscription results

**Testing SUBSCRIBE tables:**
If your implementation adds or modifies a `SUBSCRIBE` statement, complete all three steps before compiling:
1. Add `kafka` to `enabled-engines` in the test `*-package.json` — subscriptions are backed by Kafka topics even in test mode. Omitting kafka produces BUILD SUCCESS but the Subscription type will be absent from the generated schema.
2. Create a `.graphql` API test file in the test folder containing the subscription query.
3. Ensure test data includes records that trigger the subscription.

If no mutation exists to trigger the subscription, document why testing is not feasible and add a table test validating the subscription's data source instead.

## Test Data Sources

Provide test input through sources that suit the behavior under test and fit the project's existing setup. A test source should expose the table schema and event-time behavior that the SQRL pipeline expects; it does not need to mirror the production connector or require a project restructuring.

- For bounded fixture data, a filesystem source is often convenient. Choose a format supported by the connector and appropriate for the project, such as `flexible-json`, `flexible-csv`, or another suitable format.
- For streaming behavior, a `datagen` source can be useful when generated events are a better fit than files. Configure it so the test receives the records and timing needed to exercise the query.
- Reuse an existing test source or connector configuration when it already supplies the required data and semantics.
- When a query depends on event time, define the event-time column and watermark in the test source. Use fixed, ordered timestamps when deterministic window or temporal-join results are required.
- If production data comes from connector metadata or non-deterministic expressions, provide stable values that allow the test to exercise the same downstream logic.

**Test Data Guidelines:**
- Use realistic data covering relationships and joins.
- Cover edge cases and failure scenarios.
- For any filter that excludes records (e.g. `enabled = false`, reading out of bounds), include at least one record that should be excluded and verify it is absent from the output snapshot. Tests containing only passing records cannot detect broken filter logic.
- Make the data deterministic where snapshot output depends on it.
- The `timestamp-format.standard` option for `flexible-json` is required for ISO-8601 timestamps (`2024-01-01T10:00:00.000Z`) in the data file; the format's default is `SQL` (`2024-01-01 10:00:00.000`), and timestamps in the other notation fail to parse.

Before taking snapshots, the `test` command drains the Flink job, which advances all watermarks to the end, so fixtures do not need dummy sentinel records to close windows or temporal joins. Do not set `table.exec.source.idle-timeout` to more than 0 seconds in the test package, since the test runner disables idleness. If a base package sets it, override it with `"0 s"` in the test overlay. With idleness enabled, the drain can drop the last events behind chained temporal joins.

## Authentication Testing

Configure custom headers per query using `.properties` file (same name as `.graphql` file):
```text
Authorization: Bearer XYZ
```

## Test Configuration

```json
{
  "test-runner": {
    "snapshot-folder": "snapshots/myproject/", // Snapshots output directory (default: "./snapshots")
    "test-folder": "api/tests/",               // Directory containing test GraphQL queries (default: "./tests")
    "use-inferred-schema": true,               // Use inferred GraphQL schema when true, else use the one configured at "script.graphql" (default: true)
    "delay-sec": 30,                           // Max wait in sec for the job to finish before taking snapshots; ends early when the job terminates. -1 = wait until all operators are idle and required-checkpoints completed (streaming) or until the job completes (batch) (default: 30)
    "mutation-delay-sec": 0,                   // Pause(s) between mutation queries (default: 0)
    "required-checkpoints": 0,                 // Minimum completed Flink checkpoints before taking snapshots (requires delay-sec = -1)
    "create-topics": ["topic1", "topic2"],     // Kafka topics to create before tests start
    "headers": {                               // Any HTTP headers to add during the test execution. For example, JWT auth header
      "Authorization": "Bearer <token>"
    }
  }
}
```
