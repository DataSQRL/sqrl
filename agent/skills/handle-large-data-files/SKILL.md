---
name: handle-large-data-files
description: Use when the workspace contains a large data file, or when a compile/test run is blocked with LARGE_DATA_BLOCKED. Used to sample the large files down or exclude them so that DataSQRL compiles seamlessly.
---

# Handle large data files

DataSQRL copies every recognized data file (`.csv`, `.csv.gz`, `.json`, `.jsonl`, `.parquet`, `.orc`, `.avro`, ...) found **anywhere** in the project into Flink's `build/` data directory on **every** `compile` and `test` — with no cache. A large file (tens of MB or more) therefore makes every run slow and can exceed the test timeout. `compile`/`test` is blocked while such a file is present (`LARGE_DATA_BLOCKED`).

Renaming a file to `*.bak` removes it from the copy: the compiler recognizes files by data extension, and `.bak` is not one, so a `.bak` file is skipped — and it no longer triggers the block.

For each large file, first invoke the `/data-observation` skill and observe it (so you understand its schema and values), then classify and act.

# Steps to follow

## (a) The file is data a connector reads

Replace it with a small, coherent sample and point the connector at the sample.

1. Produce a small sample (<1000 rows), **keeping the same format** so the connector's `'format'` still matches:
   - For `.jsonl` / `.csv` files, use `head -1000 big.jsonl > sample.jsonl` command
   - For `.csv.gz` files, use `zcat big.csv.gz | head -1000 | gzip > sample.csv.gz` command
   - For `.parquet` files, use `duckdb -c "COPY (SELECT * FROM 'big.parquet' LIMIT 1000) TO 'sample.parquet' (FORMAT PARQUET)"` command
2. **Sample coherently across related files.** When files reference each other (foreign keys), sample a consistent set of keys so references still resolve — otherwise the sampled test data has orphaned references. Pick a set of parent keys first, then keep only the child rows for those keys.
3. Point the connector's `path` at the sample file. If you sampled into a **different** format than the original (e.g. parquet → jsonl), also update the connector's `'format'` (and any format-specific options) to match — the `'format'` key is independent of the file extension.
4. Rename the original large file to `*.bak` so it is not copied, and delete any copy already written under `build/`.

## (b) The file is only reference/sample material (no connector reads it)

You still need to observe it, but it must not be copied.

1. Invoke the `/data-observation` skill and observe a bounded prefix to learn the schema and values.
2. Rename the file to `*.bak` so it is not copied, and delete any copy already written under `build/`.
