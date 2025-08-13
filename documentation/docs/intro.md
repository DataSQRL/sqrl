# DataSQRL Documentation

DataSQRL is a framework for building data pipelines with guaranteed data integrity. It compiles SQL scripts into fully integrated data infrastructure that ingests data from multiple sources, transforms it through stream processing, and serves the results as realtime data APIs, LLM tooling, or Apache Iceberg views.

## What is DataSQRL?

DataSQRL simplifies data pipeline development by automatically generating the glue code, schemas, mappings, and deployment artifacts needed to integrate Apache Flink, Postgres, Kafka, GraphQL APIs, and other technologies into a coherent, production-grade data stack.

**Key Benefits:**
- 🛡️ **Data Integrity**: Exactly-once processing, consistent data across all outputs, automated data lineage
- 🔒 **Production-Ready**: Highly available, scalable, observable pipelines using trusted OSS technologies
- 🔗 **End-to-End Consistency**: Generated connectors and schemas maintain data integrity across the entire pipeline
- 🚀 **Developer-Friendly**: Local development, CI/CD support, comprehensive testing framework
- 🤖 **AI-Native**: Support for vector embeddings, LLM invocation, and ML model inference

## Quick Start

Check out the [**Getting Started**](getting-started) guide to build a realtime data pipeline with DataSQRL in 10 minutes.

Take a look at the [DataSQRL Examples Repository](https://github.com/DataSQRL/datasqrl-examples) for simple and complex use cases implemented with DataSQRL.

## Core Components

DataSQRL consists of three main components that work together:

### 1. [SQRL Language](sqrl-language)
SQRL extends Flink SQL with features specifically designed for reactive data processing:
- **IMPORT/EXPORT** statements for connecting data systems
- **Table functions and relationships** for interface definitions  
- **Hints** to control pipeline structure and execution
- **Subscription syntax** for real-time data streaming
- **Type system** for stream processing semantics

### 2. [Configuration](configuration) 
JSON configuration files that define:
- **Engines**: Data technologies (Flink, Postgres, Kafka, etc.)
- **Connectors**: Templates for data sources and sinks
- **Dependencies**: External data packages and libraries
- **Compiler options**: Optimization and deployment settings

### 3. [Compiler](compiler)
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
- [**SQRL Language**](sqrl-language) - Complete language specification and syntax
- [**Configuration**](configuration) - Engine setup and project configuration  
- [**Compiler**](compiler) - Command-line interface and compilation options
- [**Functions**](functions) - Built-in functions and custom function libraries

### 🔌 **Integration & Deployment**
- [**Connectors**](connectors) - Ingest from and export to external systems
- [**Concepts**](concepts) - Key concepts in stream processing (time, watermarks, etc.)
- [**How-To Guides**](howto) - Best practices and implementation patterns

### 🛠️ **Advanced Topics**
- [**Developer Documentation**](deepdive) - Internal architecture and advanced customization
- [**Compatibility**](compatibility) - Version compatibility and migration guides

## Use Cases

DataSQRL is ideal for:
- **Real-time Analytics**: Stream processing with consistent data APIs
- **Event-Driven Applications**: Reactive systems with subscriptions and alerts
- **Data Lakehouses**: Reliable Iceberg tables with automated schema management
- **LLM Applications**: Accurate data delivery for AI agents and chatbots
- **Microservices Integration**: Consistent data sharing across distributed systems

## Architecture

DataSQRL compiles your SQRL scripts into a data processing DAG that's optimized and distributed across multiple engines:

```
Data Sources → Apache Flink → PostgreSQL/Iceberg → GraphQL API
     ↓              ↓              ↓                 ↓
   Kafka        Stream         Database          Real-time
  Topics      Processing        Views             APIs
```

The compiler automatically generates all necessary:
- Flink job definitions and SQL plans
- Database schemas and views  
- Kafka topic configurations
- GraphQL schemas and resolvers
- Container and Kubernetes deployment files

## Community & Support

DataSQRL is open source and community-driven. Get help and contribute:

- 🐛 **Issues**: [GitHub Issues](https://github.com/DataSQRL/sqrl/issues)
- 💬 **Community**: [GitHub Discussions](https://github.com/DataSQRL/sqrl/discussions/)
- 🎯 **Examples**: [DataSQRL Examples Repository](https://github.com/DataSQRL/datasqrl-examples)

We welcome feedback, bug reports, and contributions to help make data pipeline development faster and more reliable for everyone.
