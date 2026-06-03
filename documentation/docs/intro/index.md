# DataSQRL Documentation

DataSQRL is an open-source **data engineering harness** that provides guardrails and feedback for AI coding agents to build reliable data pipelines, data APIs (REST, MCP, GraphQL), and data products.

DataSQRL ensures coding agents meet the **non-functional requirements** of production data systems: data quality, scalability, governance, and reliability. It provides deep-inspection of SQL, relational validators, and deterministic event-replay simulation to guide agent-generated code through iterative feedback loops.

## Why a Data Engineering Harness?

Coding agents can generate SQL queries that produce correct results on test data. But will those queries perform at scale? Handle late-arriving events correctly? Maintain data quality when upstream schemas change? Provide lineage tracking and meet compliance requirements?

These non-functional requirements, data quality, scalability, governance, reliability, cost efficiency, distinguish data engineering from general software development. General-purpose coding agents aren't equipped to handle them consistently.

A data engineering harness provides the guardrails, feedback loops, and domain-specific constraints that coding agents need. Without a harness, you get pipelines that work in demos but fail in production. With a harness, you get pipelines that embody data engineering best practices.

[Learn more about the harness architecture](/blog/agentic-data-engineering-harness) and the design choices behind DataSQRL.

## Key Capabilities

DataSQRL provides three capabilities that coding agents need to produce production-grade data systems:

### 1. Conceptual Framework

DataSQRL extends SQL to a comprehensive framework for data platforms. SQL provides the ideal foundation because it offers a mathematical foundation (relational algebra), deep introspection through its declarative nature, deterministic validation and optimization, human readability, and strong support from modern LLMs.

The framework separates **logical** and **physical** layers:
- **Logical Layer**: Expresses *what* data transformations are needed using SQRL (SQL extended with stream processing semantics)
- **Physical Layer**: Represents *how* data gets processed through engine assignment and configuration

This separation lets agents reason about business logic while the harness handles infrastructure complexity.

<img src="/img/diagrams/automation_overview.png" alt="DataSQRL Framework Overview" width="100%" />

### 2. Comprehensive Validation

DataSQRL validates at every level. From syntax and schema validation through physical plan verification to deployment asset generation:

- **Logical Validation**: Syntax, schema consistency, data flow semantics, timestamp propagation, primary key inference
- **Physical Validation**: Engine capability matching, data type mapping, topological constraint satisfaction
- **Deployment Validation**: Generated artifacts (Flink plans, Postgres schemas, GraphQL models) guaranteed consistent with logical definitions

The validation system provides comprehensive context and suggested fixes, producing better results than agents reasoning about errors independently.

### 3. Real-World Feedback

Static validation catches many issues but can't substitute for execution feedback. DataSQRL provides:

- **Simulation**: Execute pipelines locally in Docker with timestamp-accurate event replay for deterministic, reproducible testing
- **Production Telemetry**: Hooks for correlating runtime observations back to source code for autonomous troubleshooting

Since the entire pipeline is defined in SQL, it remains humanly readable and easy to verify. DataSQRL produces detailed execution plans, data lineage graphs, and optimization reports—enabling both automated analysis by agents and manual inspection by engineers.

## Quick Start

Check out the [**Getting Started**](getting-started) guide to build a data pipeline with DataSQRL and see how the test-driven feedback loop guides coding agents toward correct solutions.

Explore the [DataSQRL Examples](examples) for real-world patterns and how to setup an automated data platform with DataSQRL.

## DataSQRL Components

### 1. [SQRL Language](../sqrl-language)
SQRL extends Flink SQL to capture the complete logical layer of data pipelines:
- **IMPORT/EXPORT** statements for connecting data systems
- **Table functions and relationships** for API endpoint definitions
- **Hints** to control pipeline structure and execution
- **Subscription syntax** for real-time data streaming
- **Stream/state semantics** for temporal data processing

### 2. [Interface Design](../interface)
DataSQRL automatically generates interfaces from your SQRL script for multiple protocols:
- **Data Products** as database/data lake views and tables
- **GraphQL APIs** with queries, mutations, and subscriptions
- **REST endpoints** with GET/POST operations
- **MCP tools/resources** for AI agent integration
- **Schema customization** and operation control

### 3. [Configuration](../configuration)
JSON configuration defines the physical layer, *how* the pipeline gets executed:
- **Engines**: Data technologies (Flink, Postgres, Kafka, Iceberg, etc.)
- **Connectors**: Templates for data sources and sinks
- **Dependencies**: External data packages and libraries
- **Compiler options**: Optimization and deployment settings

### 4. [Compiler](../compiler)
The DataSQRL compiler implements validation and simulation:
- **Transpiles** SQRL scripts into deployment assets
- **Validates** logical and physical plans against data engineering constraints
- **Optimizes** data processing DAGs across multiple engines
- **Simulates** pipeline execution with deterministic event replay

## Documentation Guide

### Getting Started
- [**Getting Started**](getting-started) - Build a pipeline with the test-driven feedback loop
- [**Examples**](examples) - Practical examples for specific use cases

### Core Documentation
- [**SQRL Language**](../sqrl-language) - Complete language specification and syntax
- [**Interface Design**](../interface) - API generation and data product interfaces
- [**Configuration**](../configuration) - Engine setup and project configuration
- [**Compiler**](../compiler) - Command-line interface and compilation options
- [**Functions**](../functions) - Built-in functions and custom function libraries

### Integration & Deployment
- [**Connectors**](../connectors) - Ingest from and export to external systems
- [**Concepts**](concepts) - Key concepts in stream processing (time, watermarks, etc.)
- [**How-To Guides**](/docs/category/-how-to) - Best practices and implementation patterns

### Advanced Topics
- [**Developer Documentation**](../deepdive) - Internal architecture and advanced customization
- [**Compatibility**](../compatibility) - Version compatibility and migration guides

## Use Cases

DataSQRL enables AI-assisted automation of:
- **Data Pipelines**: Processing data in realtime or batch with production-grade reliability
- **Data APIs**: Serving processed data through REST, GraphQL, or MCP APIs
- **Data Lakehouse**: Producing Apache Iceberg tables with catalog and schema management

## Community & Support

DataSQRL is open source and community-driven:

- **Issues**: [GitHub Issues](https://github.com/DataSQRL/sqrl/issues)
- **Community**: [GitHub Discussions](https://github.com/DataSQRL/sqrl/discussions/)

We welcome feedback, bug reports, and contributions to build a data engineering harness that enables safe, reliable automation of data platforms.
