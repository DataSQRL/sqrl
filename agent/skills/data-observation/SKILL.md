---
name: data-observation
description: Use when a task provides source, sample, or test data files to inspect before modeling schemas, connectors, writing test data or implementing anything. Ensures you look at the real data (size, format, values) instead of guessing from filenames, READMEs, or prior knowledge. 
---

# Observe provided data before modeling it

Always inspect provided data (real, sample, or test data) before designing schemas, connectors, or test data. Do not infer the schema from filenames, READMEs, or prior knowledge alone. Always read the provided data (or subsets thereof if too large) first. Always consider provided data as authoritative and flag any inconsistencies with READMEs, requirements, or other provided instructions. Never resolve such inconsistencies without user direction.

For each provided data file:

1. **Check the size first.** Use the following commands: `ls -la <file>` or `du -h <file>`. The size decides how you inspect it and whether it needs special handling (step 4).
2. **Read a bounded prefix — never the whole file — in a format-aware way:**
   - Text (`.jsonl`, `.ndjson`, `.csv`, `.tsv`): use `head -100 <file>` command
   - Gzipped (`.gz`, e.g. `.csv.gz`): use `zcat <file> | head -100` command — decompress only a prefix, never the whole file
   - Columnar Parquet: use `duckdb -c "SELECT * FROM '<file>' LIMIT 100"` command. For `.orc`/`.avro`, DuckDB needs a dedicated reader (e.g. `INSTALL avro; LOAD avro;` then `read_avro('<file>')`) or use another tool — a bare `FROM 'file.orc'` will not work
   - Archives (`.zip`): use `unzip -l <file>` command to list members, then inspect a single small member
3. **Understand the data from what you actually saw:** column names and order, types, real value formats, null/optional fields, header rows, and edge cases. Base the schema and test data on the observed rows, not on assumptions.
4. **If a file is large** (roughly tens of MB or more), do **not** wire a connector to it or leave it unchanged in the project. As DataSQRL copies every data file into `build/` on each compile/test command, so a large file makes every run extremely slow (and `compile`/`test` commands used in downstream tasks will be blocked). **Invoke the `/handle-large-data-files` skill to sample it down or exclude it.**
5. **For CSV specifically:** confirm the delimiter, quoting, and header, and check that rows have a **consistent field count** — provided CSV might be ragged (trailing delimiters, extra empty columns, the odd malformed row), which passes compile and fails the Flink job at runtime. Nail the column count from the real data, then configure the source with the `/manage-connector` skill ([formats/csv.md](../manage-connector/formats/csv.md), Ragged rows).
