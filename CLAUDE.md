# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DataSQRL is a development framework for incremental and real-time data processing applications. It compiles SQL-like scripts (SQRL) into complete data pipelines that integrate Apache Kafka, Flink, PostgreSQL, Iceberg, GraphQL APIs, and LLM tooling. Built with Java 17 and Maven.

## Essential Commands

### Build Commands
```bash
# Full build and test
mvn clean install

# Build with snapshot updates
mvn clean install -P update-snapshots

# Quick build (skip tests and checks)
mvn clean install -P quickbuild

# Format code automatically
mvn -P dev initialize
```

### Testing Commands
```bash
# Unit tests only
mvn test

# Integration tests
mvn verify

# Coverage report
mvn jacoco:report

# Test specific module (ALWAYS include -Deasyjacoco.skip when using -pl)
mvn test -pl sqrl-planner -Deasyjacoco.skip

# Test specific test method in module
mvn test -pl sqrl-tools/sqrl-config -Dtest=TestClassName#testMethodName -Deasyjacoco.skip
```

### Code Quality
```bash
# Check code formatting
mvn validate-code-format

# Format code (Google Java Format)
mvn -P dev initialize
```

### Docker Commands
```bash
# Build DataSQRL CLI Docker image
docker build -t datasqrl/datasqrl-cmd .

# Run example pipeline
docker run -it --rm -p 8888:8888 -p 8081:8081 -p 9092:9092 -v $PWD:/build datasqrl/cmd:latest run example.sqrl

# Compile SQRL to deployment artifacts
docker run --rm -v $PWD:/build datasqrl/cmd:latest compile example.sqrl
```

## Architecture Overview

This is a multi-module Maven project with the following key components:

### Core Modules

**sqrl-planner/** - The compiler core that parses SQRL scripts, creates computation DAGs, optimizes them, and produces deployment artifacts. Built on Apache Calcite and Flink's parser.

**sqrl-server/** - GraphQL API server implementation:
- `sqrl-server-core/` - Generic GraphQL servlet
- `sqrl-server-vertx/` - Vert.x-based server implementation

**sqrl-tools/** - Command-line tools and utilities:
- `sqrl-cli/` - Main CLI interface (entry point: `com.datasqrl.cli.DatasqrlCli`)
- `sqrl-config/` - Configuration file handling
- `sqrl-packager/` - Dependency resolution and build preparation
- `sqrl-run/` - Pipeline execution
- `sqrl-test/` - Test execution
- `sqrl-discovery/` - Automatic schema discovery

**sqrl-testing/** - Integration tests and end-to-end pipeline testing with comprehensive test suites.

### Technology Stack
- **Java 17** with Maven build system
- **Apache Flink 1.19.2** for stream processing
- **Apache Calcite 1.27.0** for SQL parsing and optimization
- **Vert.x 5.0.0** for API server
- **GraphQL Java 19.2** for API generation
- **Apache Kafka 3.4.0** for streaming
- **PostgreSQL 42.7.7** for storage
- **JUnit 5** with Testcontainers for testing

## Development Workflow

1. **Initial Setup**: Run `mvn clean install` (required for development)
2. **Code Changes**: Use `mvn -P dev initialize` for automatic formatting
3. **Testing**: Run unit tests frequently, integration tests before commits
4. **Code Quality**: All code uses Google Java Format and requires 70% test coverage

## Git Commit Guidelines

- **Single Line Messages**: Use only single-line commit messages
- **Focus on Primary Change**: Describe the most important change made
- **No Co-authoring**: Never add co-author information to commit messages

## Key Configuration

- **Main POM**: `/pom.xml` - All dependencies and build configuration
- **Package Config**: `package.json` - DataSQRL build manifests  
- **Docker**: Multiple Dockerfiles for different components
- **Logging**: Log4j2 configuration across modules

## Testing Philosophy

- **Integration Testing**: Uses Testcontainers for PostgreSQL, Kafka, and other services
- **Snapshot Testing**: Ensures consistent output across builds
- **End-to-End Testing**: Full pipeline testing with real services
- **Coverage Requirement**: Minimum 70% instruction coverage with JaCoCo
- **Test Naming**: All new test methods must follow the `given_when_then` pattern (e.g., `givenValidConfig_whenParseConfiguration_thenReturnsExpectedResult`)

## Code Style Guidelines

- **Java 17 Features**: Use modern Java 17 syntax and language features
- **Type Inference**: Use `var` for local variables when the type is obvious from context
- **Streams API**: Prefer Java Streams over traditional loops when appropriate for readability and performance
- **Examples**:
  ```java
  // Use var for obvious types
  var config = SqrlConfig.createCurrentVersion();
  var dependencies = getDependencies();
  
  // Use Streams for collections
  var validConfigs = configs.stream()
      .filter(Config::isValid)
      .collect(Collectors.toList());
  ```

### Snapshot Files

Snapshot files are located in `sqrl-testing/sqrl-integration-tests/src/test/resources/snapshots/com/datasqrl/` and contain expected outputs for integration tests. These `.txt` files capture the complete compiled output of SQRL scripts, including:

- **Flink SQL DDL**: Stream processing table definitions and queries
- **Kafka Configuration**: Topic and serialization settings
- **PostgreSQL Schema**: Database table definitions and indexes
- **GraphQL API Schema**: Auto-generated API definitions and resolvers

**Purpose**: Snapshot testing ensures that changes to the compiler produce consistent, expected outputs. When the compiler behavior changes intentionally, snapshots must be updated using `mvn clean install -P update-snapshots`.

**Common Changes**: 
- Configuration property ordering (cosmetic changes)
- New features adding additional output artifacts
- Schema changes affecting generated SQL or GraphQL

**Debugging**: If snapshot tests fail, compare the expected vs actual output to understand how your changes affected the compiler's generated artifacts.

## Troubleshooting

### Mac Docker Issues
If TestContainers can't find Docker on Mac:
```bash
sudo ln -s $HOME/.docker/run/docker.sock /var/run/docker.sock
```

### Flink Memory Issues
If tests fail due to Flink memory issues, uncomment the configuration line in `ExecutionEnvironmentFactory.java`.

## Entry Points

- **CLI Main**: `com.datasqrl.cli.DatasqrlCli`
- **Primary Commands**: `compile`, `run`, `test`
- **GraphQL API**: Auto-generated from SQRL scripts, served at `http://localhost:8888/graphiql/`