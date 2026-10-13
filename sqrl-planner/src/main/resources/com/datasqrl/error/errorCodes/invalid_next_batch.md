`NEXT_BATCH` is used in a pipeline that enables engines other than Flink.

`NEXT_BATCH` splits a batch pipeline into sub-batches that run one after another. It is only supported when Flink is the only enabled engine.

Erroneous configuration example:
```json
{
  "enabled-engines": ["flink", "postgres", "vertx"]
}
```

Enable only Flink in `package.json`, and run Flink in batch mode:
```json
{
  "enabled-engines": ["flink"],
  "engines": {
    "flink": {
      "config": { "execution.runtime-mode": "BATCH" }
    }
  }
}
```
