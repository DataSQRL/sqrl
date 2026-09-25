# Examples Gallery

A gallery of what you can build with the DataSQRL data engineering harness, from a data product suite for a retail bank to small, self-contained pipelines across a range of use cases. Every example is open source. You can read the SQL, run it locally, and run its tests.

## Data Products for a Retail Bank

This example shows how DataSQRL works in an organizational setting. A fictional retail bank keeps a **semantic data catalog** of its source and enriched datasets. A coding agent with the DataSQRL harness uses that catalog to build **data products**: pipelines and APIs that serve specific business needs.

- **[Data Catalog](https://github.com/datasqrl-colab/finance-data-catalog-demo)**: the bank's datasets, schemas, connectors, relationships, and data quality rules
- **[Data Products](https://github.com/datasqrl-colab/finance-demo)**: two AI-generated data products built from that catalog

Explore the respective Github repositories to learn more.

## Self-Contained Examples

The **[DataSQRL Examples repository](https://github.com/DataSQRL/datasqrl-examples)** contains smaller, self-contained pipelines across a range of use cases. Each one is a standalone project with sample data and a README that explains how to run it. Pick the one closest to what you want to build:

| Example | What it builds | Look here for |
|---|---|---|
| [Getting Started Examples](https://github.com/DataSQRL/datasqrl-examples/tree/main/getting-started-examples) | Seven minimal pipelines: Kafka to console, Kafka to Kafka, stream joins, files and Kafka to Iceberg (local or AWS Glue), and Avro with Schema Registry | Connector patterns and your first pipeline |
| [Finance Credit Card Chatbot](https://github.com/DataSQRL/datasqrl-examples/tree/main/finance-credit-card-chatbot) | Enriched transaction analytics for a GenAI chatbot, a credit card rewards program, and a batch variant that writes spending views to Iceberg for DuckDB and Snowflake | Enrichment, APIs for AI agents, batch to Iceberg |
| [Clickstream AI Recommendation](https://github.com/DataSQRL/datasqrl-examples/tree/main/clickstream-ai-recommendation) | Personalized content recommendations from clickstream data and LLM-generated vector embeddings | Vector embeddings and real-time recommendations |
| [Healthcare Study](https://github.com/DataSQRL/datasqrl-examples/tree/main/healthcare-study) | Three use cases over one shared catalog: a real-time API, analytics in Iceberg, and an enriched stream published to Kafka | One catalog feeding API, analytics, and streaming |
| [Oil & Gas Agent Automation](https://github.com/DataSQRL/datasqrl-examples/tree/main/oil-gas-agent-automation) | A monitoring API for an AI agent plus an operations backend with an ingest mutation and a low-flow-rate alert subscription | Event-triggered agents and subscriptions |
| [IoT Sensor Metrics](https://github.com/DataSQRL/datasqrl-examples/tree/main/iot-sensor-metrics) | An event-driven microservice that ingests sensor readings and serves metrics and alerts | Ingest APIs and time-windowed metrics |
| [Logistics Shipping](https://github.com/DataSQRL/datasqrl-examples/tree/main/logistics-shipping-geodata) | Real-time shipment tracking with locations, in about 30 lines of SQL | A compact streaming pipeline |
| [Law Enforcement](https://github.com/DataSQRL/datasqrl-examples/tree/main/law-enforcement) | An integrated view of drivers, vehicles, warrants, and BOLOs, with analytics and alerts for traffic stops | Combining databases and streams |
| [Iceberg Data Deduplication](https://github.com/DataSQRL/datasqrl-examples/tree/main/iceberg-data-deduplication) | Compaction and deletion jobs for Iceberg tables | Maintaining data lake tables |
| [User-Defined Functions](https://github.com/DataSQRL/datasqrl-examples/tree/main/user-defined-function) | A custom function shipped with JBang or a Maven project | Extending SQL with your own logic |

The repository also includes a [data generator](https://github.com/DataSQRL/datasqrl-examples/tree/main/data-generator) for producing larger datasets for experiments and benchmarks.

## Build Your Own

Start from your own catalog or data sources and describe the data product you need. The [Getting Started guide](getting-started) shows how to set up the DataSQRL agent.
