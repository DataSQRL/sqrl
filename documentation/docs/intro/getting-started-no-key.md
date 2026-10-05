# Getting Started with DataSQRL

This tutorial demonstrates how the DataSQRL data engineering harness works without an LLM API key. That means we won't be running an agent that autonomously uses the harness to build data projects, but walk an agent through the steps and use the harness to ensure correct, safe, and easy to understand results.

## Prerequisites

You'll need:

- **Docker** installed and running
- A terminal (macOS/Linux: Terminal, Windows: PowerShell or WSL)
- [Optional] A coding agent (Claude Code, Codex, Gemini CLI, Copilot, or similar)

### Install Docker

If you don't already have Docker:

- **macOS**: [Download Docker Desktop for Mac](https://www.docker.com/products/docker-desktop/)
- **Windows**: [Download Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/)
- **Linux**: Use your package manager (e.g., `sudo apt install docker.io`)

Verify Docker is working:

```bash
docker --version
```

## Create New Project

Create a new data project with the `init` command in an empty folder:

```bash
docker run --rm -v $PWD:/workspace datasqrl/cmd init api messenger
```
(Use `${PWD}` in Powershell on Windows)

This creates a data API project called `messenger` for processing posted messages with sample data sources and a processing script called `messenger.sqrl`.

The engines executing the pipeline are defined in the `package.json` files:
![Initial Pipeline Architecture](/img/diagrams/getting_started_diagram1.png)

## Run the Pipeline

Execute the SQRL project:

```bash
docker run -it --rm -p 8888:8888 -p 8081:8081 -v $PWD:/workspace datasqrl/cmd run messenger-prod-package.json
```

Access the GraphQL API at [http://localhost:8888/v1/graphiql/](http://localhost:8888/v1/graphiql/).

Add a message:
```graphql
mutation {
    Messages(event: {message: "Hello World"}) {
        message_time
    }
}
```

Query messages:
```graphql
{
    Messages {
        uuid
        message
        message_time
    }
}
```

Terminate with `CTRL-C`.

## Let Agents Extend the Pipeline

:::note
This step requires access to a coding agent. Load the [DataSQRL skills](https://github.com/DataSQRL/sqrl/tree/main/agent/skills) into your agent so it knows how to write DataSQRL. If you don't have a coding agent, you can make the edits to `messenger.sqrl` by hand and run the same commands.
:::


Now instruct your coding agent to extend `messenger.sqrl`. For example:

> "Add an endpoint that returns the total message count and the timestamp of the most recent message. Include test coverage."

The agent should modify `messenger.sqrl` and iterate using the test command:

```bash
docker run -it --rm -v $PWD:/workspace datasqrl/cmd test messenger-test-package.json
```

This feedback loop is how DataSQRL guides agents toward correct solutions. The test command:
- Compiles the SQRL script and validates semantics
- Runs the pipeline in simulation with timestamp-accurate event replay
- Compares results against snapshot expectations

The first time a new test runs, it creates a snapshot. Subsequent runs validate against that snapshot. When tests fail, the compiler provides actionable error messages that help agents refine their solution.

A correct implementation might look like:

```sql
TotalMessages := SELECT COUNT(*)          AS num_messages, 
                        MAX(message_time) AS latest_timestamp
                 FROM Messages LIMIT 1;
```

## Add Real-Time Subscriptions

Ask your agent to add a subscription for error messages:

> "Add a subscription that pushes messages containing the word 'error' to consumers in real-time."

The agent should add something like:

```sql
AlertMessages := SUBSCRIBE SELECT * FROM Messages WHERE LOWER(message) LIKE '%error%';
```

Run the production version to test subscriptions:
```bash
docker run -it --rm -p 8888:8888 -p 8081:8081 -v $PWD:/workspace datasqrl/cmd run messenger-prod-package.json
```

In GraphiQL, start a subscription:
```graphql
subscription {
    AlertMessages {
        uuid
        message
        message_time
    }
}
```

In a new browser tab, add an error message:
```graphql
mutation {
    Messages(event: {message: "I found an ERROR! Oh no"}) {
        message_time
    }
}
```

The subscription tab should show the message pushed through in real-time.

## Compile for Deployment

Build deployment artifacts:
```bash
docker run --rm -v $PWD:/workspace datasqrl/cmd compile messenger-prod-package.json
```

The `build/deploy/plan` directory contains:
- Flink compiled plans
- Kafka topic definitions
- PostgreSQL schemas and views
- Server queries and GraphQL models

The `build` directory also includes files useful for inspection and verification:
- `pipeline_visual.html`: Visual representation of the pipeline DAG
- `pipeline_explain.txt`: Textual DAG representation for coding agents
- `inferred_schema.graphqls`: Generated GraphQL schema

![DataSQRL Pipeline Visualization](/img/screenshots/dag_messenger.png)

Click nodes in the visualization to inspect schema, logical plan, and physical plan details. The deployment artifacts support human validation of pipeline correctness and quality. You can use them to build an ensemble of judges to provide automatic validation of compliance, governance, and reliability requirements.

## Next Steps

You've seen how DataSQRL provides the feedback loop that coding agents need to build production-grade data pipelines. The test command validates agent-generated code, the compiler provides actionable errors, and the simulator ensures real-world correctness.

As shown in the [main Getting Started tutorial](getting-started), you can extend the DataSQRL harness to build custom agents that can complete many data engineering tasks with high quality and according to your organization's rules and guidelines.

Next:
- **[Full Documentation](/docs/intro)**: Explore guides to DataSQRL's concepts, tools, configuration, and deployment
- **[Tutorials](examples)**: Learn by building more complex pipelines
- **[Example Projects](https://github.com/DataSQRL/datasqrl-examples)**: See real-world patterns in action

## Troubleshooting

- **Ports already in use**: Check if 8888 or 8081 is being used by another app
- **Agent not understanding SQRL**: Share the [SQRL Language Reference](/docs/sqrl-language) with your agent
- **Test failures**: Review the error output—DataSQRL provides specific guidance on what to fix
