# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DataSQRL is a development framework for incremental and real-time data processing applications.
It compiles SQL-like scripts (SQRL) into complete data pipelines that integrate Kafka, Flink, PostgreSQL,
Iceberg, GraphQL APIs, and LLM tooling. Built with Java 17 and Maven.

## External Dependencies

The following repositories contain additional runtime components:
* [Flink SQL Runner](https://github.com/DataSQRL/flink-sql-runner): Runs the Flink compiled plan and provides additional utilities for Flink
* [SQRL K8s](https://github.com/DataSQRL/sqrl-k8s): A template for running DataSQRL pipelines in Kubernetes

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

# Server-specific builds
mvn clean package                    # Build fat JAR (vertx-server.jar)
mvn clean package -Pskip-shade-plugin  # Build without fat JAR
mvn clean package -Pinstrument     # Build with JaCoCo instrumentation
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

# Container tests (requires Docker images to be built)
mvn -B install -DonlyContainerE2E -pl :sqrl-container-testing -Dit.test=TestClassName

# Container tests with dev profile (recommended)
mvn -B install -Pdev -Dit.test=TestClassName

# Run all container tests (omit -pl when testing container code changes)
mvn -B install -DonlyContainerE2E -Dit.test=*ContainerIT
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

**sqrl-server/** - GraphQL API server implementation that translates GraphQL queries, mutations, and subscriptions into database calls:
- `sqrl-server-core/` - Core interfaces and models (GraphQL schema, execution coordinates)
- `sqrl-server-vertx-base/` - Full Vert.x implementation with database clients and Kafka integration
- `sqrl-server-vertx/` - Standalone deployment module with Docker support

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

## Maven Version Management

### Version Properties Pattern
All dependency versions should be centralized as properties in the root pom.xml (`/pom.xml`) to ensure consistency across all modules.

**Root POM Properties Location**: `/pom.xml` - All version properties are defined in the `<properties>` section.

**Key Principles**:
- **NEVER use hardcoded versions in child module pom.xml files** - This is a strict requirement
- **ALWAYS add new version properties to the root pom.xml when introducing new dependencies** - No exceptions
- **Use consistent property naming**: `<libraryname.version>X.Y.Z</libraryname.version>`
- **All existing hardcoded versions must be migrated** to use centralized properties immediately
- **Plugin versions must also follow this pattern** - Add plugin version properties to root POM

**Examples of Existing Properties**:
```xml
<properties>
  <jackson.version>2.19.1</jackson.version>
  <vertx.version>5.0.1</vertx.version>
  <kafka.version>3.4.0</kafka.version>
  <flink.version>1.19.3</flink.version>
  <httpcomponents.version>4.5.14</httpcomponents.version>
  <jjwt.version>0.12.6</jjwt.version>
  <testcontainers.version>1.21.3</testcontainers.version>
</properties>
```

**Child Module Usage**:
```xml
<dependency>
  <groupId>org.apache.httpcomponents</groupId>
  <artifactId>httpclient</artifactId>
  <version>${httpcomponents.version}</version>
</dependency>
```

**When Adding New Dependencies**:
1. **MANDATORY**: First add the version property to root pom.xml
2. Then reference the property in child module pom.xml files
3. This ensures version consistency across all modules and makes version upgrades centralized

**Version Management Enforcement**:
- **Code reviews must reject any hardcoded dependency versions** in child modules
- **All new dependencies must use version properties** from the root POM
- **When updating existing dependencies**, always ensure they use centralized version properties
- **Plugin versions follow the same pattern** as dependency versions

## Git Commits

- **Commit Messages**: Use succinct single-line messages describing the most important change
- **Issue References**: Include issue links on second line if provided in the change prompt
- **Co-authorship**: Do not add Claude as co-author unless explicitly requested
- **Commit Best Practice**: Always use `-s` and `-S` flags when committing to sign-off and sign commits cryptographically

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
- **Test Assertions**: Use AssertJ (`org.assertj.core.api.Assertions`) for all test assertions. Avoid JUnit's `org.junit.jupiter.api.Assertions` in favor of AssertJ's more fluent and readable API

### Container Testing

Container tests in `sqrl-container-testing` validate the end-to-end functionality of DataSQRL Docker images:

- **Purpose**: Test the complete Docker image deployment including compilation and server startup
- **Requirements**: Docker must be running and DataSQRL images must be built (`datasqrl/cmd:local`, `datasqrl/sqrl-server:local`)
- **Test Structure**: Tests extend `SqrlContainerTestBase` which provides container management utilities
- **Available Endpoints**: 
  - `/graphql` - Main GraphQL API endpoint
  - `/health` - Health check endpoint (returns 204 No Content when healthy)
  - `/metrics` - Prometheus metrics endpoint (availability depends on configuration)
- **Common Patterns**: Compile SQRL script → Start server container → Execute HTTP requests → Validate responses
- **Test Data**: Uses test cases from `sqrl-integration-tests/src/test/resources/usecases/`

## Code Style Guidelines

- **Java 17 Features**: Use modern Java 17 syntax and language features
- **Type Inference**: Use `var` for local variables when the type is obvious from context
- **Streams API**: Prefer Java Streams over traditional loops when appropriate for readability and performance
- **Lombok Usage**: Prefer Lombok annotations to reduce boilerplate code:
  - `@Slf4j` for logging instead of manual logger declarations
  - `@SneakyThrows` for checked exception handling where appropriate
  - `@Data`, `@Value`, `@Builder` for data classes
  - `@RequiredArgsConstructor`, `@AllArgsConstructor` for constructors
- **File Formatting**: All new files must end with an empty line
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

### CLI and Commands
- **CLI Main**: `com.datasqrl.cli.DatasqrlCli`
- **Primary Commands**: `compile`, `run`, `test`, `execute`
- **GraphQL API**: Auto-generated from SQRL scripts, served at `http://localhost:8888/graphiql/`

### Server Entry Points
- **Server Main**: `com.datasqrl.graphql.SqrlLauncher` - Main class for standalone deployment
- **Main Verticle**: `com.datasqrl.graphql.GraphQLServerVerticle` - Main Verticle that configures the GraphQL server

### Core Server Classes
- **RootGraphqlModel**: Central model class encapsulating GraphQL schema and execution coordinates
- **GraphQLEngineBuilder**: Builds GraphQL engine by wiring schema, resolvers, and custom scalars
- **QueryExecutionContext**: Context for query execution

## Server Architecture Details

### Key Design Patterns
- **Visitor Pattern**: Extensively used for processing GraphQL model (`RootVisitor`, `QueryCoordVisitor`, `SchemaVisitor`)
- **Reactive Architecture**: Built on Vert.x event loop with CompletableFuture for async operations
- **Schema-First**: GraphQL schema loaded from `server-model.json` at runtime with pre-compiled execution paths

### Runtime Model
The server operates on a compiled model where the DataSQRL compiler generates `server-model.json` containing all GraphQL execution metadata. The server loads this at startup and creates optimized execution paths - no runtime SQL generation occurs.

### Database Abstraction
Multi-database support through `SqlClient` interface:
- PostgreSQL: Native Vert.x client with pipelining
- DuckDB: JDBC-based connection
- Snowflake: JDBC-based connection with specialized configuration

### Server Configuration Files
- `server-model.json`: Runtime GraphQL model and execution coordinates
- `server-config.json`: Server configuration (ports, database connections)
- `snowflake-config.json`: Optional Snowflake-specific configuration
- `log4j2.properties`: Logging configuration

## Important Development Notes

### Module Dependencies
Always check existing dependencies in `pom.xml` files before adding new libraries. The project uses specific versions of Vert.x, GraphQL-Java, and database drivers.

### Database Operations
All database operations are asynchronous and non-blocking. Use the appropriate `SqlClient` implementation for the target database system.

### GraphQL Schema Modifications
Schema changes require regenerating the `server-model.json` file through the DataSQRL compiler. The server does not support runtime schema modifications.
