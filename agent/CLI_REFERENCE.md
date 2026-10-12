# DataSQRL CLI Reference

Run every DataSQRL command through `/opt/agent/cmd.sh`. It resets the local Postgres and Redpanda state, blocks oversized data files, serializes concurrent calls, and ends with `COMPILE_DONE: exit_code=<n>`.

## Package configuration layering

Commands that take package files merge them **in the order given**: later files override fields of earlier ones, objects are deep-merged, and arrays are replaced wholesale. The built-in defaults are applied first.

Projects use a shared base plus thin per-environment overlays (see the `configure-sqrl` skill):

```
<project>-shared-package.json   # common settings: version, enabled-engines, script.main, engines
<project>-test-package.json     # test overlay: test-runner, test-only overrides
<project>-prod-package.json     # production overlay
```

Always pass the base first and the overlay second. The CLI default (`package.json`) rarely exists, so always name the package files explicitly.

## Common options

| Option                        | Meaning                                                                                                   |
|-------------------------------|-----------------------------------------------------------------------------------------------------------|
| `-r, --project-root=<dir>`    | Project root, relative path. Inferred from the package paths if omitted. Use `-r .` from the project root. |
| `-b, --build=<dir>`           | Subfolder of `<project-root>/build` for build output, e.g. `-b fraud_store` for a sub-project.            |
| `-t, --target=<dir>`          | Folder for deployment artifacts. Defaults to `<build-folder>/deploy`.                                     |
| `-B, --batch-output`          | Disable colored output.                                                                                   |

## Commands

### init

Creates a new project with `<name>.sqrl`, `<name>-prod-package.json`, and `<name>-test-package.json`. `<type>` is `stream`, `dataset`, or `api`; `--batch` selects Flink batch mode.

```bash
/opt/agent/cmd.sh init api myproject
```

### add-func

Adds a Java UDF template to the `functions/` folder of an existing project. `--aggregate` creates an aggregate function instead of a scalar one.

```bash
/opt/agent/cmd.sh add-func MyFunction
```

### compile

Compiles the project and writes `build/pipeline_explain.txt`, `build/pipeline_visual.html`, and the deployment artifacts in `build/deploy` (plans in `build/deploy/plan`).

```bash
/opt/agent/cmd.sh compile -r . myproject-shared-package.json myproject-prod-package.json
```

### test

Compiles and runs the pipeline in simulation, drains the Flink job, executes the GraphQL tests and `/*+ test */` tables, and compares the results against the snapshots. New tests create a snapshot; if no other test failed, the pipeline is reset and re-run in the same invocation to verify the new snapshots, and the test passes if the results match.

```bash
/opt/agent/cmd.sh test -r . myproject-shared-package.json myproject-test-package.json
```

### run

Compiles and runs the pipeline with all engines locally. The API is served at `http://localhost:8888/v1/graphiql/`. `run` keeps running until it is terminated, so prefer `test` for verification.

```bash
/opt/agent/cmd.sh run -r . myproject-shared-package.json myproject-prod-package.json
```

### exec

Runs an already compiled project from its existing build artifacts without recompiling. It takes no package files.

```bash
/opt/agent/cmd.sh exec -r .
```

## Sub-projects

When several sub-projects share a repository, give each its own build folder so their outputs do not overwrite each other:

```bash
/opt/agent/cmd.sh test -r . fraud-shared-package.json fraud_store-test-package.json -b fraud_store
```
