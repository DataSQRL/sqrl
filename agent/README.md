# DataSQRL coding agent

This directory defines a baseline interactive coding agent for DataSQRL projects. It combines the DataSQRL CLI with [Pi](https://pi.dev), DataSQRL-specific instructions, and skills for building data pipelines, connectors, configuration, and APIs.

The image starts Pi in `/workspace`. Mount a project there, provide a model-provider API key, and work with the agent from the terminal.

## Contents

| File or directory                | Purpose                                                                                                                   |
|----------------------------------|---------------------------------------------------------------------------------------------------------------------------|
| [`Dockerfile`](Dockerfile)       | Builds the agent image on top of a DataSQRL CLI image. Installs Pi, then packages the agent content.                      |
| [`entrypoint.sh`](entrypoint.sh) | Starts interactive Pi and selects a provider from explicit settings or available API keys.                                |
| [`cmd.sh`](cmd.sh)               | Runs DataSQRL CLI commands with fresh local engine state and large-data protection. Agents use it as `/opt/agent/cmd.sh`. |
| [`AGENTS.md`](AGENTS.md)         | Always-loaded instructions: the working directory, skill-selection rules, and verification expectations.                  |
| [`CLI_REFERENCE.md`](CLI_REFERENCE.md) | DataSQRL CLI commands, options, and package configuration layering, referenced from `AGENTS.md`.                    |
| [`skills/`](skills/)             | On-demand DataSQRL knowledge loaded by Pi when a task matches a skill description.                                        |
| `/opt/datasqrl-examples/`        | Optional read-only mount of custom or [datasqrl-examples](https://github.com/DataSQRL/datasqrl-examples) reference implementations, used by relevant skills when useful. |

## Build and start

Build from the repository root:

```bash
docker build -t core-agent -f agent/Dockerfile .
```

Start the agent in a project directory. Pi uses the provider key from the environment and opens an interactive terminal session:

```bash
docker run --rm -it \
  -e OPENAI_API_KEY \
  -v "$PWD:/workspace" \
  core-agent
```

### Reference examples

The image does not include example projects. Mount a local checkout when the
agent should use them as read-only reference implementations:

```bash
docker run --rm -it \
  -e OPENAI_API_KEY \
  -v "$PWD:/workspace" \
  -v /path/to/datasqrl-examples:/opt/datasqrl-examples:ro \
  core-agent
```

Mount any curated or project-specific examples directory at this path. Skills
use its contents only when relevant and do not assume a particular project is
present. When no useful mount is available and a public reference is needed,
they clone the public examples repository shallowly into `/tmp`; the clone is
never written into the user's project.

For an Anthropic model, pass `ANTHROPIC_API_KEY` instead:

```bash
docker run --rm -it \
  -e ANTHROPIC_API_KEY \
  -v "$PWD:/workspace" \
  core-agent
```

To select a model explicitly, set `PI_MODEL`. `PI_PROVIDER` overrides provider inference when needed:

```bash
docker run --rm -it \
  -e OPENAI_API_KEY \
  -e PI_MODEL=gpt-5.1-codex \
  -v "$PWD:/workspace" \
  core-agent
```

## How the agent uses knowledge

Pi reads [`AGENTS.md`](AGENTS.md) at startup, so place instructions that should affect every task there. It discovers each `skills/*/SKILL.md` directory and exposes its name and description to the model. The model loads a skill when the request matches its description or when the user explicitly requests it.

Use `AGENTS.md` for concise, durable operating rules: project layout, commands to run, safety constraints, and the order of work. Use a skill for specialized workflows or reference material that only applies to certain tasks. Keep a skill self-contained: its `SKILL.md` identifies when to use it, while its sibling files contain the detailed reference material it links to.

## Extend the core agent

Create a small image that extends the baseline when you need organization- or project-specific knowledge. Copy skills into Pi's global skill directory and replace `AGENTS.md` only when your custom version includes the baseline guidance you still want to retain.

```dockerfile
FROM core-agent:latest

COPY company-skills/ /root/.pi/agent/skills/
COPY company-AGENTS.md /root/.pi/agent/AGENTS.md
```

For additional system tools or libraries, add them in the derived image. Keep runtime dependencies separate from skills so instruction-only changes do not reinstall packages:

```dockerfile
FROM core-agent:latest

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends jq \
    && rm -rf /var/lib/apt/lists/*

COPY company-skills/ /root/.pi/agent/skills/
```

Use the same `/workspace` mount and provider-key environment variables when starting a custom image. Do not bake API keys, credentials, or production connection settings into an image; provide them only at runtime.

## Verify a custom image

After building a derived image, confirm that Pi and the expected skills are present:

```bash
docker run --rm --entrypoint bash my-data-agent -lc \
  'pi --version && find /root/.pi/agent/skills -name SKILL.md'
```

Then start an interactive session against a disposable or representative project and ask the agent to inspect the project and run an appropriate DataSQRL command through `/opt/agent/cmd.sh`.
