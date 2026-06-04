# DataSQRL Examples

These examples demonstrate how AI coding agents use DataSQRL to build production-grade data products from data catalog definitions. In practical implementations, an autonomous data platform maintains a central data catalog that agents reference to build data products, data pipelines, and data APIs.

## Commerce Example

A retail commerce scenario with customer orders, products, and inventory data.

- **[Data Catalog](https://github.com/datasqrl-colab/datasqrl-workshop-catalog)**: Defines the data sources, schemas, and connectors for the commerce domain
- **[Sample Data Products](https://github.com/datasqrl-colab/datasqrl-workshop-sample)**: AI-generated data pipelines and APIs built from the catalog

## Finance Example

A banking scenario with accounts, transactions, and customer data.

- **[Data Catalog](https://github.com/datasqrl-colab/finance-data-catalog-demo)**: Defines the data sources, schemas, and connectors for the finance domain
- **[Sample Data Products](https://github.com/datasqrl-colab/finance-demo)**: AI-generated data pipelines and APIs built from the catalog

## How It Works

Each example follows the same pattern:

1. **Data Catalog**: Defines available data sources with schemas, connectors, and sample data for testing
2. **Agent Implementation**: A coding agent uses the catalog to build data products, iterating with DataSQRL's test command until tests pass
3. **Deployment Artifacts**: DataSQRL compiles the SQRL scripts into production-ready Flink plans, Kafka topics, Postgres schemas, and GraphQL APIs

These examples show how DataSQRL's feedback loop guides agents toward correct, production-grade implementations.
