# Getting Started with DataSQRL

The easiest way to understand how DataSQRL guides and provides feedback for agentic development of data pipelines is to build a data pipeline yourself. We are going to build a pipeline for message processing.

<!-- Add video tutorial -->
## Prerequisites

You'll need:

- **Docker** installed
- A terminal (macOS/Linux: Terminal, Windows: PowerShell or WSL)
- Basic understanding of SQL

### Install Docker

If you don't already have Docker:

- **macOS**: [Download Docker Desktop for Mac](https://www.docker.com/products/docker-desktop/)
- **Windows**: [Download Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/)
- **Linux**: Use your package manager (e.g., `sudo apt install docker.io`)

Verify Docker is working:

```bash
docker --version
```

Make sure Docker is running before continuing.

## Create New Project

To create a new data project with DataSQRL, use the `init` command in an empty folder.

```bash
 docker run --rm -v $PWD:/build datasqrl/cmd init api messenger
```
(Use `${PWD}` in Powershell on Windows).

This creates a new data API project called `messenger` with some sample data sources and a simple data processing script called `messenger.sqrl`.

The script defines a simple HelloWorld table that exposes the imported messages. The source of the messages is a managed Kafka topic in production and static Json data for testing.

The engines executing the pipeline are defined in the `package.json` files and create the following architecture:
![Initial Pipeline Architecture](/img/diagrams/getting_started_diagram1.png)

## Run SQRL Script
We can now execute the SQRL project with the compiler:

```bash
docker run -it --rm -p 8888:8888 -p 8081:8081 -v $PWD:/build datasqrl/cmd run messenger-prod-package.json
```

Note, that we are mapping the local directory so the compiler has access to the script. We are also mapping a number of ports, so we have access to the API and visibility into the pipeline.



## Access the API

The pipeline is exposed through a GraphQL API that you can access at  [http://localhost:8888/v1/graphiql/](http://localhost:8888/v1/graphiql/) in your browser.

To add a new message, run this mutation
```graphql
mutation {
    Messages(event: {message: "Hello World"}) {
        message_time
    }
}
```
Copy the query into the left-hand panel and click on the "play" button to see the result.

To query the aggregated statistics, run the following query:
```graphql
{
    Messages {
        uuid
        message
        message_time
    }
}
```

Once you are done, terminate the pipeline with `CTRL-C`.

## Extend the SQRL Script

Let's add more data processing to our script. Copy the following SQL code into `messenger.sqrl`:

```sql
TotalMessages := SELECT COUNT(*) as num_messages, MAX(message_time) as latest_timestamp
                 FROM Messages LIMIT 1;

AlertMessages := SUBSCRIBE SELECT * FROM Messages WHERE LOWER(message) LIKE '%error%';
```

First, we are aggregating total messages. Next, we are adding a subscription for messages that contain the word `error`. These messages get pushed to consumers immediately. Finally, we are adding a test case for the total messages.

To run the test cases, use this command:
```bash
docker run -it --rm -v $PWD:/build datasqrl/cmd test messenger-test-package.json
```

The first time you run a new test case, you will see a failure when the snapshot is created. When you run the test again, it will pass.

The `AlertMessages` get pushed out via the API. Run the production version:
```bash
docker run -it --rm -p 8888:8888 -p 8081:8081 -v $PWD:/build datasqrl/cmd run messenger-prod-package.json
```

In the GraphQL IDE, execute the following subscription
```graphql
subscription {
    AlertMessages {
        uuid
        message
        message_time
    }
}
```
*While* the subscription is running, open a new browser tab for [GraphiQL](http://localhost:8888/v1/graphiql/) and execute this mutation:
```graphql
mutation {
    Messages(event: {message: "I found an ERROR! Oh no"}) {
        message_time
    }
}
```
If you toggle back to the subscription tab, you should see the message getting pushed through.


## Deployment

To build the deployment assets in the for the data pipeline, execute
```bash
docker run --rm -v $PWD:/build datasqrl/cmd compile messenger-prod-package.json
``` 
The `build/deploy/plan` directory contains the Flink compiled plan, Kafka topic definitions, PostgreSQL schema and view definitions, server queries, and GraphQL data model.

The `build` directory contains additional files that are useful to inspect, verify, and understand the physical model for the data pipeline:
* `pipeline_visual.html` is a visual representation of the entire data pipeline. Open this file in your browser.
* `pipeline_explain.txt` contains a textual representation of the pipeline DAG that DataSQRL generates for AI coding agents.
* `inferred_schema.graphqls` contains the generated GraphQL schema for the API.

![DataSQRL Pipeline Visualization](/img/screenshots/dag_messenger.png)

The picture above is the visualization of the pipeline we have build thus far. You can click on the individual nodes in the graph to inspect the schema, logical, and physical plan.

## Next Steps

Congratulations, you build a production-grade data pipeline and saw how DataSQRL generates the physical pipeline model from the logical definition of the data processing.
We used the testing framework to simulate real-world execution of the pipeline and used snapshots validating the results.

Together, those three elements - the conceptual framework with the logical (SQL) and physical (dataflow DAG) model, the compiler and validator, and the simulator - comprise the world model of DataSQRL. This world model provides feedback and guardrails to coding agents for autonomous development of data pipelines and data products. Now, you can pair up DataSQRL with your favorite coding agent and let it do the work for you.

Next, check out:
* **[Full Documentation](/docs/intro)** for the complete reference, language spec, and more.
* **[Tutorials](tutorials)** if you prefer learning by doing.

## Troubleshooting Common Issues

- **Ports already in use**: Check if 8888 or 8081 is being used by another app
