# Developer Documentation

:::note
The developer documentation assumes familiarity with DataSQRL and only explains the codebase and organization.
Read the user [documentation](datasqrl.github.io/sqrl) first.
:::

The DataSQRL project consists of two parts:

* The DataSQRL build tool: Compiles SQRL scripts to data pipelines
* The DataSQRL runtime modules: libraries and components that are executed
  as part of a compiled data pipeline when the deployment assets are deployed or tested.

Modules that are part of the build tool depend on runtime modules, but not vice versa. The goal is to keep runtime modules lean, efficient, and minimal for runtime robustness.

This repository contains the entire build tool implementation and parts of the runtime split across multiple modules.
Refer to the README.md file in each module for information on what this module contains and what its purpose is.


## DataSQRL Build Tool

* Planner: The DataSQRL compiler and planner is implemented in the [sqrl-planner](sqrl-planner) module.
* Tooling: CLI commands and Packager for preparing a SQRL project to build are implemented in the [sqrl-tools](sqrl-tools) module and sub-modules.
* Integration Tests: Integration tests that cover the entire compilation process and running the generated data pipelines are implemented in [sqrl-testing](sqrl-testing).

Click on the links above to read the documentation for the respective modules.

## DataSQRL Runtime

* Server: The default server implementation based on Vert.x is implemented in the [sqrl-server](sqrl-server) modules.
* Run & Test: Runtime infrastructure for executing SQRL pipelines in the docker image and running tests, implemented in the [sqrl-run](sqrl-tools/sqrl-run) and [sqrl-test](sqrl-tools/sqrl-test) modules, respectively.
* SQRL Functions: FlinkSQL scalar functions that are used by DataSQRL pipelines internally are implemented in the [sqrl-functions](sqrl-functions) module. Note that user accessible functions should be implemented in the [Flink SQL Runner](https://github.com/DataSQRL/flink-sql-runner).

The following repositories contain additional runtime components:
* [Flink SQL Runner](https://github.com/DataSQRL/flink-sql-runner): Runs the Flink compiled plan and provides additional utilities for Flink.
* [SQRL K8s](https://github.com/DataSQRL/sqrl-k8s): A template for running DataSQRL pipelines in Kubernetes
