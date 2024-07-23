# Postgres Log profile

When using this profile the log engine will be postgres instead of the default kafka.

## Example

The `postgres-log/create-table` test can be used to test this feature, located here:
```
sqrl/sqrl-testing/sqrl-integration-tests/src/test/resources/usecases/postgres-log/create-table
```

In the above directory, issue this command:
```
compile --profile "/path/to/profiles/default"  --profile "/path/to/profiles/postgres_log"
```