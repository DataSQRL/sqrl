---
name: sqrl-codereview-guide
description: DataSQRL code review guidelines
triggers:
  - /codereview
---

# SQRL Code Review Guidelines

# Repository Code Review Guidelines

You are reviewing code for SQRL. Follow these guidelines:

## Review Decisions

### When to APPROVE
- Change is not touching snapshot files
- Change that contains snapshot updates and snapshot content update looks good based on the core logic changes 
- Change is not user-impacting, or it is user-impacting, and the relevant documentation and agent skills are updated accordingly

### When to COMMENT
- A snapshot change contains updates that are undesirable
- A previously successful snapshot now contains a failure and the core logic does not indicate why
- A previously failing snapshot now contains a success and the core logic does not indicate why
- A user-impacting change is not reflected in the documentation (`documentation/docs`) and/or the agent skills (`agent/skills`)
- Documentation or agent skill updates contradict the behavior implemented by the code change

## Core Principles

1. **SQRL Snapshot Files**: Any file with a `.snapshot` extension that contains the snapshot data
2. **Unit Test Snapshot Files**: Any `.txt` files under the `sqrl-testing/sqrl-testing-integration/src/test/resources/snapshots/com/datasqrl` directory
3. **User Documentation**: Markdown files under `documentation/docs`, published as the DataSQRL documentation site
4. **Agent Skills**: The `SKILL.md` files and their reference files under `agent/skills`, used by the DataSQRL data engineering agent

## What to Check

- **[Failing Snapshot]**: A previously succeeding snapshot now contains a failure and the core logic changes do not indicate that
- **[Succeeding Snapshot]**: A previously failing snapshot now contains a success and the core logic changes do not indicate that
- **[Snapshot Data Changes]**: A snapshot file contains updates (e.g., new data columns, changed format) that are not related to the core logic
- **[Missing Docs Update]**: A user impacting change has no corresponding update in `documentation/docs`
- **[Missing Skill Update]**: A user impacting change has no corresponding update in `agent/skills`
- **[Stale Docs or Skills]**: Existing docs or skills still describe the old behavior, reference removed or renamed options, commands, or functions, or contain examples that no longer work

## Documentation and Skills Consistency

A change is **user impacting** if it changes anything a DataSQRL user or the DataSQRL agent can observe or rely on, for example:
- SQRL language syntax, semantics, hints, or error messages that users act on
- CLI commands, flags, arguments, or their behavior
- `package.json` configuration keys, defaults, or validation
- Connectors, formats, or engine (Flink, Postgres, Kafka, Iceberg, Vert.x, etc.) behavior and settings
- System or library functions, and the UDF authoring model (including JBang UDFs)
- Generated API (GraphQL, REST, MCP) schema, naming, authorization, or server behavior
- Compilation output, deployment artifacts, test/run behavior, or version compatibility

Purely internal changes (refactorings, test-only changes, CI/build changes, dependency bumps without behavior change) are **not** user-impacting and need no docs or skill updates.

For every user impacting change, verify that **both** the documentation and the agent skills are updated. The only exception is behavior that exists solely in the DataSQRL agent image (`agent/`, e.g. checks in `agent/cmd.sh`) and is not part of the compiler, CLI, or server: such changes are not covered by the user documentation and require only an agent skill update. These are the rows marked "- (agent only)" below. Use the following mapping to find the places that should change:

| Change area                               | Documentation (`documentation/docs`)                                                 | Agent skills (`agent/skills`)                                                            |
|-------------------------------------------|--------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------|
| SQRL language, hints, semantics           | `sqrl-language.md`, `deepdive.md`, `intro/concepts.md`                               | `implement-sqrl/SKILL.md`, `implement-sqrl/guides/`                                      |
| Functions (system/library)                | `functions.md`, `functions-*-generated.md`, `stdlib-docs/`                           | `implement-sqrl/functions.md`, `implement-sqrl/functions/`                               |
| UDFs (Java, JBang)                        | `functions.md`                                                                       | `implement-udf/SKILL.md`                                                                 |
| `package.json` configuration and defaults | `configuration.md`, `configuration-default.md`                                       | `configure-sqrl/SKILL.md`                                                                |
| Engine settings                           | `configuration-engine/*.md`                                                          | `configure-sqrl/*.md` (matching engine file)                                             |
| Connectors and formats                    | `connectors.md`                                                                      | `manage-connector/` (`connectors/`, `formats/`), `configure-sqrl/connector-templates.md` |
| API (GraphQL, REST, MCP), authorization   | `interface.md`                                                                       | `design-api/`                                                                            |
| CLI commands and flags                    | `compiler.md`, `intro/getting-started*.md`                                           | `init-sqrl/`, `test-sqrl/`, any skill invoking the changed command                       |
| Testing behavior                          | `compiler.md`                                                                        | `test-sqrl/SKILL.md`                                                                     |
| Compilation output, deployment            | `compilation-output.md`, `deployment.md`, `configuration-engine/cloud-deployment.md` | `configure-sqrl/cloud-deployment.md`                                                     |
| Agent large data file blocking            | - (agent only)                                                                       | `handle-large-data-files/SKILL.md`                                                       |
| Version upgrades (Flink, etc.)            | `compatibility.md`                                                                   | any skill referencing the version (e.g. `//DEPS` in `implement-udf`)                     |

The mapping is a starting point, not an exhaustive list. Search `documentation/docs` and `agent/skills` for the names of changed options, commands, functions, or error codes to find every affected location.

When checking, keep in mind:
- `configuration-default.md` is generated from `sqrl-planner/src/main/resources/default-package.json` by `documentation/docs/configuration-default-update.sh`. If defaults change, check that it was regenerated rather than asking for manual edits.
- The engine docs under `documentation/docs/configuration-engine` and the engine reference files under `agent/skills/configure-sqrl` cover the same topics; an update to one usually requires an update to the other.
- Skills are written for an agent: verify that examples are correct and copy-pasteable, and that the guidance does not instruct the agent to use removed or renamed features.
- New features should be discoverable: a new option, function, or command should appear in the docs and in the skill an agent would load for that task.

When commenting about missing or stale docs and skills, name the specific files that should be updated and briefly describe what needs to change.
