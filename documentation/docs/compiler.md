# DataSQRL Command

The DataSQRL command initializes, compiles, runs, and tests SQRL projects.

You invoke the DataSQRL command in your terminal or command line.
Choose your operating system below or use Docker which works on any machine that has Docker installed.

## Installation

Always pull the latest Docker image to ensure you have the most recent updates:

```bash
docker pull datasqrl/cmd:latest
```

## Command Overview

The DataSQRL CLI provides the following commands:

```bash
docker run --rm -v $PWD:/build datasqrl/cmd -h
```

```
Usage: sqrl [-hV] [COMMAND]
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  init      Initializes an empty SQRL project.
  add-func  Adds a new function definition into the 'functions' folder of an
              existing project.
  compile   Compiles an SQRL project and produces all build artifacts.
  test      Compiles, then tests a SQRL project.
  run       Compiles, then runs a SQRL project in a lightweight, standalone
              environment.
  exec      Executes an already compiled SQRL script using its existing build
              artifacts.
```

:::warning
You need to mount the current directory (`$PWD` or `${PWD}` on Windows with Powershell) to the `/build`
directory for file access in Docker.
:::

:::info
Note that most commands accept package configuration files as arguments to configure all aspects of the project.
See the [configuration documentation](configuration.md) for more details.
:::

## Init Command

The `init` command creates a new SQRL project with the necessary files and directory structure.

```bash
docker run --rm -v $PWD/<project-folder>:/build datasqrl/cmd init -h
```

```
Usage: sqrl init [-hV] [--batch] <projectType> <projectName>
Initializes an empty SQRL project.
      <projectType>   Project type. Valid values: STREAM, DATASET, API.
      <projectName>   Project name — used as the base name for both the SQRL
                        script and the package configuration files.
      --batch         Use BATCH mode in Flink.
  -h, --help          Show this help message and exit.
  -V, --version       Print version information and exit.
```

### Example

```bash
mkdir my-project

docker run --rm -v $PWD/my-project:/build datasqrl/cmd init stream my-project
```

This creates a new streaming project named `my-project` with the default configuration and directory structure.

## Add-Func Command

The `add-func` command adds a new user-defined function (UDF) to an existing SQRL project.

```bash
docker run --rm -v $PWD/<project-folder>:/build datasqrl/cmd add-func -h
```

```
Usage: sqrl add-func [-hV] [--aggregate] <fnName>
Adds a new function definition into the 'functions' folder of an existing
project.
      <fnName>      Name of the function.
      --aggregate   Adds an aggregate function instead of the default scalar
                      one.
  -h, --help        Show this help message and exit.
  -V, --version     Print version information and exit.
```

### Example

```bash
mkdir my-project

docker run --rm -v $PWD/my-project:/build datasqrl/cmd add-func MyAwesomeFunction
```

This creates a new scalar function template in the `functions/` directory.
Use `--aggregate` to create an aggregate function instead.

## Compile Command

The `compile` command processes a SQRL project into a deployable data pipeline.
It stages all files needed by the compiler in the `build` directory and outputs the created deployment
artifacts for all engines in the target folder (defaults to `build/deploy`).

```bash
docker run --rm -v $PWD:/build datasqrl/cmd compile -h
```

```
Usage: sqrl compile [-BhV] [-t=<targetFolder>] [<packageFiles>...]
Compiles an SQRL project and produces all build artifacts.
      [<packageFiles>...]   Package configuration file(s) of the project.
                              Default: "package.json".
  -B, --batch-output        Run in batch output mode (disables colored output).
  -h, --help                Show this help message and exit.
  -t, --target=<targetFolder>
                            Target folder for deployment artifacts and plans.
                              Default: "build/deploy".
  -V, --version             Print version information and exit.
```

### Example

```bash
cd my-project

# Defaults to package.json
docker run --rm -v $PWD:/build datasqrl/cmd compile
```

Or with a specific package configuration file:

```bash
cd my-project

docker run --rm -v $PWD:/build datasqrl/cmd compile package-prod.json
```

### Output

Upon successful compilation, the compiler writes:
- Data processing DAG to `build/pipeline_explain.txt`
- Visual representation to `build/pipeline_visual.html` (open in browser to inspect the DAG)
- Deployment artifacts to the target folder

## Run Command

The `run` command compiles and runs the generated data pipeline in Docker.

```bash
docker run --rm -it -p 8081:8081 -p 8888:8888 -p 9092:9092 -v $PWD:/build datasqrl/cmd run -h
```

```
Usage: sqrl run [-BhV] [-t=<targetFolder>] [<packageFiles>...]
Compiles, then runs a SQRL project in a lightweight, standalone environment.
      [<packageFiles>...]   Package configuration file(s) of the project.
                              Default: "package.json".
  -B, --batch-output        Run in batch output mode (disables colored output).
  -h, --help                Show this help message and exit.
  -t, --target=<targetFolder>
                            Target folder for deployment artifacts and plans.
                              Default: "build/deploy".
  -V, --version             Print version information and exit.
```

:::info
Note the additional port mappings to access the individual data systems that are running the pipeline.
Additionally, `run` loads [configuration settings](https://raw.githubusercontent.com/DataSQRL/sqrl/refs/heads/main/sqrl-planner/src/main/resources/default-run-package.json)
that are applicable exclusively during runtime.
These `run`-specific configuration options will be replaced if they are defined in any custom package configuration file.
:::

### Example

```bash
cd my-project

docker run --rm -it -p 8081:8081 -p 8888:8888 -p 9092:9092 --rm -v $PWD:/build datasqrl/cmd run
```

This compiles, then runs the SQRL project in the `my-project` folder,
exposing the Flink UI on port 8081, the Vert.x server on port 8888, and Redpanda on port 9092. 

### Engines

The run command uses the following engines:
* Flink as the stream engine: The Flink cluster is accessible through the WebUI at [http://localhost:8081/](http://localhost:8081/).
* Postgres as the transactional database engine
* Iceberg+DuckDB as the analytic database engine
* Redpanda as the log engine: The Redpanda cluster is accessible on port 9092 (via Kafka command line tooling).
* Vertx as the server engine: 
  * The GraphQL API is accessible at [http://localhost:8888/v1/graphiql/](http://localhost:8888/v1/graphiql/).
  * The Swagger UI for the REST API is accessible at [http://localhost:8888/v1/swagger-ui](http://localhost:8888/v1/swagger-ui)
  * The MCP API is accessible at `http://localhost:8888/v1/mcp/`

### Data Access

DataSQRL runs up the data systems listed above and maps your local directories for data access.
To access this data in your DataSQRL jobs during local execution use:
* `${KAFKA_BOOTSTRAP_SERVERS}` to connect to the Redpanda Kafka cluster
* `${DATA_PATH}/` to reference `.jsonl` or `.csv` data in your project.

This allows DataSQRL to map connectors correctly and also applies to [testing](#test-command).

### Data Persistence

To preserve inserted data between runs, mount a directory for Redpanda to persist the data to:

```bash
docker run -it -p 8081:8081 -p 8888:8888 -p 9092:9092 --rm -v /mydata/project:/data/redpanda -v $PWD:/build datasqrl/cmd run my-package.json
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

## Test Command

The test command compiles and runs the data pipeline, then executes the provided test API queries and API endpoints
for all tables annotated with `/*+ test */` to snapshot the results.

When you first run the test command or add additional test cases, it will create the snapshots and fail.
All subsequent runs of the test command compare the results to the previously snapshotted results and succeed
if the results are identical, else fail.

```bash
docker run --rm -it -p 8081:8081 -p 8888:8888 -p 9092:9092 -v $PWD:/build datasqrl/cmd test -h
```

```
Usage: sqrl test [-BhV] [-t=<targetFolder>] [<packageFiles>...]
Compiles, then tests a SQRL project.
      [<packageFiles>...]   Package configuration file(s) of the project.
                              Default: "package.json".
  -B, --batch-output        Run in batch output mode (disables colored output).
  -h, --help                Show this help message and exit.
  -t, --target=<targetFolder>
                            Target folder for deployment artifacts and plans.
                              Default: "build/deploy".
  -V, --version             Print version information and exit.
```

### Example

```bash
docker run --rm -it -p 8081:8081 -p 8888:8888 -p 9092:9092 -v $PWD:/build datasqrl/cmd test
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

## Exec Command

The exec command executes an already compiled SQRL project using its existing build artifacts, without recompiling.
This is useful when you want to run a previously compiled pipeline.

```bash
run --rm -it -p 8081:8081 -p 8888:8888 -p 9092:9092 -v $PWD:/build datasqrl/cmd exec -h
```

```
Usage: sqrl exec [-BhV] [-t=<targetFolder>] [<packageFiles>...]
Executes an already compiled SQRL script using its existing build artifacts.
      [<packageFiles>...]   Package configuration file(s) of the project.
                              Default: "package.json".
  -B, --batch-output        Run in batch output mode (disables colored output).
  -h, --help                Show this help message and exit.
  -t, --target=<targetFolder>
                            Target folder for deployment artifacts and plans.
                              Default: "build/deploy".
  -V, --version             Print version information and exit.
```

### Example

```bash
cd my-project

docker run --rm -it -p 8081:8081 -p 8888:8888 -p 9092:9092 -v $PWD:/build datasqrl/cmd exec
```
