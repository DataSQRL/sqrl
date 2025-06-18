# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SQRL Server is the API server component of DataSQRL, built with Java 17 and Eclipse Vert.x. It translates GraphQL queries, mutations, and subscriptions into database calls against configured data systems (PostgreSQL, Kafka, DuckDB, Snowflake).

## Development Commands

### Build
```bash
mvn clean compile                    # Compile all modules
mvn clean package                    # Build fat JAR (vertx-server.jar)
mvn clean package -Pskip-shade-plugin  # Build without fat JAR
```

### Testing
```bash
mvn test                            # Run unit tests
mvn test -Dtest=ClassName           # Run specific test class
mvn test -Dtest=ClassName#methodName # Run specific test method
mvn verify                          # Run integration tests
```

### Code Coverage
```bash
mvn clean package -Pinstrument     # Build with JaCoCo instrumentation
```

### Docker
```bash
docker build -t sqrl-server .      # Build container image
```

## Architecture

### Multi-Module Structure
- **sqrl-server-core**: Core interfaces and models (GraphQL schema, execution coordinates)
- **sqrl-server-vertx-base**: Full Vert.x implementation with database clients and Kafka integration
- **sqrl-server-vertx**: Standalone deployment module with Docker support

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

### Configuration Files
- `server-model.json`: Runtime GraphQL model and execution coordinates
- `server-config.json`: Server configuration (ports, database connections)
- `snowflake-config.json`: Optional Snowflake-specific configuration
- `log4j2.properties`: Logging configuration

## Key Classes and Entry Points

### Main Entry Points
- `com.datasqrl.graphql.SqrlLauncher`: Main class for standalone deployment
- `com.datasqrl.graphql.GraphQLServerVerticle`: Main Verticle that configures the GraphQL server

### Core Model Classes
- `RootGraphqlModel`: Central model class encapsulating GraphQL schema and execution coordinates
- `GraphQLEngineBuilder`: Builds GraphQL engine by wiring schema, resolvers, and custom scalars
- `QueryExecutionContext`: Context for query execution

### Testing Framework
- JUnit 5 with Vert.x JUnit 5 integration
- Testcontainers for database integration testing
- Integration tests use `*IT` naming convention

## Important Development Notes

### Module Dependencies
Always check existing dependencies in `pom.xml` files before adding new libraries. The project uses specific versions of Vert.x, GraphQL-Java, and database drivers.

### Database Operations
All database operations are asynchronous and non-blocking. Use the appropriate `SqlClient` implementation for the target database system.

### GraphQL Schema Modifications
Schema changes require regenerating the `server-model.json` file through the DataSQRL compiler. The server does not support runtime schema modifications.