---
name: init-sqrl
description: Use when starting a new DataSQRL project or initializing an empty pipeline. Use for greenfield implementations, project scaffolding, or setting up the folder structure.
modes: [planning, implementation, setup, question]
---

Follow these steps to initialize a DataSQRL project:
1. When an available examples directory contains a useful starting point, review its `README.md` and relevant projects. Treat its contents as optional reference material, not required templates.
2. Adapt relevant example files into the working directory only when they fit the task. Don't overwrite existing files.
3. Review provided source/sink table definitions in sub-folders to identify the sqrl files that define the sources and sinks required for the implementation. If the requirements reference a provided data catalog, include that catalog and delete copied connectors unless the requirements specifically ask to create additional sources/sinks outside the catalog.
4. Update copied files to match user requirements by:
   * **ALWAYS** invoke the relevant skill before updating or implementing anything
   * Use user-provided source/sink definitions from sub-folders or an included data catalog if they exist (include/import them, reusing what exists), otherwise update the template connectors. Invoke the `/manage-connector` skill (Connector Organization) before writing or editing any source/sink definition.
   * Updating the SQRL files to use those sources & sinks and adjust implementation by matching the code to the updated table schemas and removing features that are not required. Invoke the `/implement-sqrl` skill before writing or significantly modifying any SQRL logic during this step.
   * When renaming the main `.sqrl` script, update `script.main` key in the (sub-)project's base config so every package.json reference matches the actual filename before the first compile. When creating or updating any configuration, invoke `/configure-sqrl` skill.
   * Update the project configuration if necessary.
   * Update the GraphQL API definition and defined operations to match the new schema.
   * Create or update the configuration files: a `*-shared-package.json` base plus `*-<env>-package.json` thin per-environment overlays. Invoke the `/configure-sqrl` skill (Base Config and Environment Overlays).
   * Add `/*+test */` annotated SQL test queries at the end of the main SQRL script(s) for each exposed table. These test queries should SELECT from the table with a WHERE filter on a known test data value and produce deterministic results. Invoke the `/test-sqrl` skill (Writing Tests) for the test and test-data rules, then delete the template's existing snapshots and run the tests to generate fresh ones (Running Tests, Snapshot Lifecycle).
   * Create or update the test runner script `run-tests.sh` which is the entry point for the project's tests. Invoke the `/test-sqrl` skill for details.
   * Update `README.md` with user requirements and project description. Add a list of features that are required by the user but not part of the template and need to be implemented
