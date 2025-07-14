# DataSQRL Command

The DataSQRL command compiles, runs, and tests SQRL scripts.

You invoke the DataSQRL command in your terminal or command line.
Choose your operating system below or use Docker which works on any machine that has Docker installed.

## Installation

Always pull the latest Docker image to ensure you have the most recent updates:

```bash
docker pull datasqrl/cmd:latest
```

### Global Options
All commands support the following global options:

| Option/Flag Name  | Description                                                                                                                                                                                                                         |
|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| -c or --config    | 	Specifies the path to one or more package configuration files. Contents of multiple files are merged in the specified order. Defaults to package.json in the current directory, generating a default configuration if none exists. |

Note, that most commands require that you either specify the SQRL script (and, optionally, a GraphQL schema)
as command line arguments or use the `-c` option to specify a project configuration file that allows you to configure
all aspects of the project. See the [configuration documentation](configuration.md) for more details.

## Compile Command

The compile command processes a SQRL script and, optionally, an API specification, into a deployable data pipeline.
The compile command stages all files needed by the compiler in the `build` directory and output the created deployment
artifacts for all engines in the `build/deploy` folder.



```bash
docker run --rm -v $PWD:/build datasqrl/cmd compile myscript.sqrl
```
or
```bash
docker run --rm -v $PWD:/build datasqrl/cmd compile -c package.json
```

Note, that you need to mount the current directory (`$PWD` or `${PWD}` on windows with Powershell) to the `/build`
directory for file access in docker.


| Option/Flag Name | Description                                                                                                                   |
|------------------|-------------------------------------------------------------------------------------------------------------------------------|
| -a or --api      | Generates an API specification (GraphQL schema) in the file schema.graphqls. Overwrites any existing file with the same name. |
| -t or --target   | Directory to write deployment artifacts, defaults to build/plan.                                                              |

Upon successful compilation, the compiler writes the data processing DAG that is planned from the SQRL script into the
file `build/pipeline_explain.txt` and a visual representation to `build/pipeline_visual.html`.
Open the latter file in your browser to inspect the data processing DAG.


## Run Command

The run command compiles and runs the generated data pipeline in docker.


```bash
docker run -it -p 8888:8888 -p 8081:8081 -p 9092:9092 --rm -v $PWD:/build datasqrl/cmd run myscript.sqrl
```

Note, the additional port mappings to access the individual data systems that are running the pipeline.
Additionally, `run` loads [configuration settings](#run-specific-default-configuration) that are applicable exclusively during runtime. 

The run command uses the following engines:
* Flink as the stream engine: The Flink cluster is accessible through the WebUI at [http://localhost:8081/](http://localhost:8081/).
* Postgres as the transactional database engine
* Iceberg+DuckDB as the analytic database engine
* RedPanda as the log engine: The RedPanda cluster is accessible on port 9092 (via Kafka command line tooling).
* Vertx as the server engine: The GraphQL API is accessible at [http://localhost:8888/graphiql/](http://localhost:8888/graphiql/).

### Data Access

DataSQRL runs up the data systems listed above and maps your local directories for data access.
To access this data in your DataSQRL jobs during local execution use:
* `${PROPERTIES_BOOTSTRAP_SERVERS}` to connect to the Redpanda Kafka cluster
* `${DATA_PATH}/` to reference `.jsonl` or `.csv` data in your project.

This allows DataSQRL to map connectors correctly and also applies to [testing](#test-command).

### Data Persistence

To preserve inserted data between runs, mount a directory for RedPanda to persist the data to:


```bash
docker run -it -p 8888:8888 -p 8081:8081 -p 9092:9092 --rm -v /mydata/project:/data/redpanda -v $PWD:/build datasqrl/cmd run myscript.sqrl
```

The volume mount contains the data written to the log engine and persists it to the local `/mydata/project` directory
where you want to store the data (adjust as needed and make sure the directory exists).


When you terminate (via `CTRL-C`) and re-run your SQRL project, it will replay prior data.

### Deployment 

The run command is primarily used for local development and quick iteration cycles. It supports small-scale deployments.
For large-scale deployments, we recommend that you run the generated pipeline in Kubernetes by extending our [Kubernetes setup](https://github.com/DataSQRL/sqrl-k8s).

If you prefer a managed service, you can use [DataSQRL Cloud](https://www.datasqrl.com/) for automated and optimized deployments.
Alternatively, you can deploy the generated deployment artifacts in the `build/plan` directory using available managed
services by your preferred cloud provider.

### Run-specific Default Configuration

The following configuration are only applied for the `run` command.
It is important to note, that these configuration options will also be replaced if they are defined in any
custom `package.json` that is given via the [`-c` compiler config option](#global-options).

```json
{
  "engines" : {
    "flink" : {
      "config": {
        "state.checkpoints.dir": "file:///data/flink/checkpoints",
        "state.savepoints.dir": "file:///data/flink/savepoints"
      }
    }
  }
}
```

## Test Command

The test command compiles and runs the data pipeline, then executes the provided test API queries and API endpoints
for all tables annotated with `/*+test */` to snapshot the results.

When you first run the test command or add additional test cases, it will create the snapshots and fail.
All subsequent runs of the test command compare the results to the previously snapshotted results and succeed
if the results are identical, else fail.

```bash
docker run --rm -v $PWD:/build datasqrl/cmd test
```

The Test Command related configuration can be adjusted via the [`test-runner`](configuration.md#test-runner-test-runner) configuration.
The `tests` directory contains GraphQL queries that are executed against the API of the generated data pipeline. 

### Test Execution Overview

Subscriptions are registered first. This ensures that any incoming data events are captured as soon as they occur.

Next, mutations are executed in alphabetical order. This controlled ordering allows predictable state changes during testing.

Queries are executed after all mutations have been applied. This step retrieves the system state resulting from the preceding operations.

Once all queries complete, the subscriptions are terminated and the data collected during their active phase is assembled into snapshots.

:::warning
Subscriptions can only be tested in conjunction with mutations at this time.
:::
