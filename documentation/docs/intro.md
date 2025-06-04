# DataSQRL Documentation

DataSQRL is a data streaming framework that simplifies the development of data pipelines for data engineers by providing an integrated framework that automates the data plumbing that holds multiple stages of a pipeline together. 
DataSQRL compiles SQL scripts into integrated data pipelines.

There are 3 components to building data pipelines with DataSQRL:

* **[SQRL Language](sqrl-language)**: SQRL extends Flink SQL (an ANSI SQL compatible dialect) with IMPORT/EXPORT statements for connecting data systems and code modularity, table functions and relationships for interface definitions, hints to control pipeline structure, execution, and access, and doc-strings for semantic annotations. See the full [SQRL language specification](sqrl-language).
* **[SQRL Configuration](configuration)**: A JSON configuration file that defines and configures the data technologies to execute the pipeline (called `engines`), data dependencies, compiler options, connector templates, and execution values. See the full [list of configuration options](configuration).
* **[DataSQRL Compiler](compiler)**: The compiler transpiles the SQRL scripts and connector definitions according to the configuration into deployment assets for the engines. It also executes the pipeline for quick iterations and runs test manually or as part of a CI/CD pipeline. See the full [list of compiler command options](compiler)

## Functions

DataSQRL uses SQL to define the structure and processing of data pipelines augmented by [function libraries](functions).

SQRL extends the standard SQL function catalog with additional functionality. In addition, you can import function libraries or implement your own functions.

[Learn more about the functions](functions) SQRL supports out-of-the box and how to implement your own.

## Deployment

You can use the DataSQRL docker image with the [`run` command](compiler#run-command) for local, demo, and non-production deployments.
For production deployments, use Kubernetes or hosted cloud services.

The [DataSQRL Kubernetes repository](https://github.com/DataSQRL/sqrl-k8s) contains a Helm chart template for deploying DataSQRL compiled pipelines to Kubernetes using the engine Kubernetes operators and a basic terraform setup for the Kubernetes cluster.

<!--
[DataSQRL Cloud](https://www.datasqrl.com) is a managed service that runs DataSQRL pipelines with no operational overhead and integrates directly with GitHub for simple deployments.
-->

## Additional Resources

* Use [Connectors](connectors) to ingest data from and sink data to external systems
* Read the [Tutorials](tutorials) for practical examples.
* Check out the [How-To Guides](howto) for useful tips & tricks.
* Learn about the [Concepts](concepts) underlying stream processing.
* Read the [Developer Documentation](deepdive) to learn more about the internals.

## Community & Support

We aim to enable data engineers to build data pipelines quickly and eliminate the data plumbing busy work. Your feedback is invaluable in achieving this goal. Let us know what works and what doesn't by filing GitHub issues or in the [DataSQRL Slack community](https://join.slack.com/t/datasqrlcommunity/shared_invite/zt-2l3rl1g6o-im6YXYCqU7t55CNaHqz_Kg).
