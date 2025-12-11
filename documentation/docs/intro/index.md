# DataSQRL Documentation

DataSQRL is a data automation framework for building reliable data pipelines, data APIs (REST, MCP, GraphQL), and data products in SQL using open-source technologies.
It provides a logical and physical world model, validator and planner, and an execution and testing framework to provide guardrails and feedback for AI coding agents.
DataSQRL has a modular architecture to support different types of data systems like Apache Kafka, Apache Iceberg, PostgreSQL, DuckDB, and others.
It can be customized and extended for specific data platform automation requirements.

## What is DataSQRL?

DataSQRL provides the foundational components needed by AI agents to produce safe, reliable data pipelines:
* A world model to "understand" data processing
* A validation framework to ensure quality solutions
* A simulator to provide reproducible feedback on real-world data flows for correctness.

### World Model

DataSQRL extends SQL to a comprehensive world model for data platforms. SQL provides the ideal foundation for automation because it offers a mathematical foundation (relational algebra),
deep introspection through its declarative nature, deterministic validation and optimization, human readability, and strong support from modern LLMs.
This SQL-based world model enables AI to understand your entire data landscape - from source to sink - including schemas, connectors, data transformations, and execution semantics.
DataSQRL produces the computational graph and deployment assets for inspection by AI in iterative refinement loops.

<img src="/img/diagrams/automation_overview.png" alt="SQRL Timeline" width="100%" />

### Simulation & Verification

DataSQRL provides a complete runtime and testing framework that executes pipelines locally in Docker, enabling rapid iteration with 100% reproducible results.
The testing framework supports snapshot tests and assertions to verify pipeline correctness, spot regressions in CI/CD, and provide real-world feedback to AI in iterative refinement loops.
Since the entire data pipeline is defined in SQL, it remains humanly readable and easy to verify.
DataSQRL produces detailed execution plans showing how the computation DAG is distributed across engines, comprehensive data lineage graphs tracking data flow from source to sink, and optimization reports explaining compiler decisions.
These artifacts enable both automated analysis by AI agents to improve solutions and manual inspection by developers to ensure correctness, making AI-assisted data platform automation trustworthy and production-safe.

### Key Benefits
- 🛡️ **Data Consistency Guarantees**: Exactly-once processing, data consistency across all outputs, schema alignment, and data lineage tracking
- 🔒 **Production-Grade Reliability**: Robust, highly available, scalable, secure, access-controlled, and observable data pipelines
- 🚀 **Developer Workflow Integration**: Local development, quick iteration with feedback, CI/CD support, and comprehensive testing framework

[Learn more about](/blog/data-platform-automation) the design choices, science, and the "Why?" behind DataSQRL.

## Quick Start

Check out the [**Getting Started**](getting-started) guide to build a realtime data pipeline with DataSQRL in 10 minutes.

Take a look at the [DataSQRL Examples Repository](https://github.com/DataSQRL/datasqrl-examples) for simple and complex use cases implemented with DataSQRL.

## DataSQRL Components

DataSQRL's world model consists of three components.

### 1. [SQRL Language](../sqrl-language)
SQRL extends Flink SQL to capture the complete data model and entire processing and retrieval logic of data pipelines:
- **IMPORT/EXPORT** statements for connecting data systems
- **Table functions and relationships** for interface definitions
- **Hints** to control pipeline structure and execution
- **Subscription syntax** for real-time data streaming
- **Type system** for stream processing semantics

### 2. [Interface Design](../interface)
DataSQRL automatically generates interfaces from your SQRL script for multiple protocols with customization support:
- **Data Products** as database/data lake views and tables
- **GraphQL APIs** with queries, mutations, and subscriptions
- **REST endpoints** with GET/POST operations
- **MCP tools/resources** for AI agent integration
- **Schema customization** and operation control

### 3. [Configuration](../configuration)
JSON configuration file defines **how** the data pipeline gets executed:
- **Engines**: Data technologies (Flink, Postgres, Kafka, etc.)
- **Connectors**: Templates for data sources and sinks
- **Dependencies**: External data packages and libraries
- **Compiler options**: Optimization and deployment settings

DataSQRL's verification and simulation is implemented by the compiler.

### 4. [Compiler](../compiler)
The DataSQRL compiler:
- **Transpiles** SQRL scripts into deployment assets
- **Optimizes** data processing DAGs across multiple engines
- **Generates** schemas, connectors, and API definitions
- **Executes** pipelines locally for development and testing

## Documentation Guide

### 🚀 **Getting Started**
- [**Getting Started**](getting-started) - Complete tutorial with hands-on examples
- [**Tutorials**](tutorials) - Practical examples for specific use cases

### 📚 **Core Documentation**
- [**SQRL Language**](../sqrl-language) - Complete language specification and syntax
- [**Interface Design**](../interface) - API generation and data product interfaces
- [**Configuration**](../configuration) - Engine setup and project configuration
- [**Compiler**](../compiler) - Command-line interface and compilation options
- [**Functions**](../functions) - Built-in functions and custom function libraries

### 🔌 **Integration & Deployment**
- [**Connectors**](../connectors) - Ingest from and export to external systems
- [**Concepts**](concepts) - Key concepts in stream processing (time, watermarks, etc.)
- [**How-To Guides**](/docs/category/-how-to) - Best practices and implementation patterns

### 🛠️ **Advanced Topics**
- [**Developer Documentation**](../deepdive) - Internal architecture and advanced customization
- [**Compatibility**](../compatibility) - Version compatibility and migration guides

## Use Cases

The goal of DataSQRL is automating data platforms with a focus on:
- **Data Pipelines**: processing data in realtime or batch
- **Data APIs**: serving processed data through REST, GraphQL, or MCP APIs.
- **Data Lakehouse**: producing Apache Iceberg tables with catalog and schema management

## Community & Support

DataSQRL is open source and community-driven. Get help and contribute:

- 🐛 **Issues**: [GitHub Issues](https://github.com/DataSQRL/sqrl/issues)
- 💬 **Community**: [GitHub Discussions](https://github.com/DataSQRL/sqrl/discussions/)
- 🎯 **Examples**: [DataSQRL Examples Repository](https://github.com/DataSQRL/datasqrl-examples)

We welcome feedback, bug reports, and contributions to empower anyone to build safe, reliable data pipelines by automating the grunt work.
