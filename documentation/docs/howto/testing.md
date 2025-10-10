# Testing

DataSQRL's automated test runner can execute two types of snapshot test via the [`run` command](../compiler#test-command):

1. Test tables annotated with the `/*+test */` hint
2. GraphQL operations in the `test-folder` configured under the `test-runner` in the [`package.json`](../configuration).

You can combine both types of tests. All snapshots are written to the same `snapshot-folder`.

## Modularizing Sources

In order to separate test data from the real-world data sources of the pipeline, move all `CREATE TABLE` statements into a `connectors/source-prod.sqrl` file which gets imported into the main SQRL script with:

```sql
IMPORT connectors.source-{{variant}}.*; -- or you can import into a separate namespace and adjust references
```

Note, that `{{variant}}` is a template variable that is used to switch out the source definition and needs to be defined in the [`package.json`](../configuration):
```json
{
  "script": {
    "main": "my-project.sqrl",                         
    "config": { 
      "variant": "prod"
    }
  }
}
```

## Creating Test Configuration

With the sources modularized out, create a separate test configuration (e.g. `my_project-test-package.json`) for the project:
```json
{
  "script": {
    "main": "my-project.sqrl",                         
    "config": { 
      "variant": "test"
    }
  }
}
```

## Test Data Sources

Now, create a new sqrl script for the test data sources `connectors/source-test.sqrl` which defines the same tables as the production sources but using static test data in `.jsonl` files. Generate a JSONL file for each table and place it in the `connectors/test-data` folder. Then define the tables based on the originals:

```sql
CREATE TABLE MyTable (
    ...same columns as original source but replacing METADATA and non-deterministic columns regular columns and data ...
    WATERMARK FOR `my_timestamp` AS `my_timestamp` - INTERVAL '0.001' SECOND
)
    WITH (
        'format' = 'flexible-json',
        'path' = '${DATA_PATH}/my_table.jsonl',
        'source.monitor-interval' = '10000', -- remove for batch sources
        'connector' = 'filesystem'
        );
```

Refer to the [connectors documentation](../connectors) for more information on how to source table connectors.

Guidelines for generating test data:
* Generate realistic test data that covers the relationship/joins that you want to test
* Make sure the test data covers common failure scenarios and edge cases
* Set a static event timestamp on test data so that test execution is deterministic
* If the original `CREATE TABLE` definition used `METADATA` columns, convert those to regular columns and add static test data
* If the original `CREATE TABLE` definition used non-deterministic computed columns (e.g. `NOW()`), convert those to regular columns and add static test data.
* To advance the watermark during test execution, add a dummy record at the end with an event timestamp that is much larger than all test records' timestamps to ensure the watermark advances enough to close windows and flush out all events. 

## API Tests

Optionally, define GraphQL mutations, subscriptions, and queries in `.graphql` files inside the configured `test-folder` for `test-runner`. Those queries must be valid queries against the GraphQL schema for the compiled pipeline.

## Run Tests Locally

Use the [`test` command](../compiler#test-command) to execute the tests.
The first time you run the tests, snapshots are created in the configured `snapshot-folder`. Validate that those snapshots are correct.
Subsequent executions of the test will compare snapshots and fail if they are unequal.

## Automate Testing

Check the snapshots into the version control system alongside the code and configure your build or CI/CD tool to run the tests automatically.

## Update Tests

As you encounter production issues or discover failures during manual testing, convert those scenarios to test data and execute them as part of your test suite to avoid regressions.
By setting the event timestamp explicitly, you can precisely recreate a failure scenario and replay it deterministically.