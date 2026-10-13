The `package.json` configuration is invalid and could not be loaded.

This happens when a configuration file is not valid JSON, does not match the configuration schema (e.g. an unknown engine name or a value of the wrong type), or declares an unsupported `version`.

Erroneous code example:
```json
{
  "version": 2,
  "enabled-engines": ["flink", "postgress", "vertx"]
}
```

Check the message above for the offending file and field. The current configuration `version` is `1`, and `enabled-engines` must only list supported engines:
```json
{
  "version": 1,
  "enabled-engines": ["flink", "postgres", "vertx"]
}
```
