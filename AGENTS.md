# DataSQRL

This file provides guidance to coding agents (e.g., Claude Code, Codex) when working with code in this repository.

## Project Overview

DataSQRL is an open-source data engineering harness for building data engineering agents designed around
human control, correctness, and safety. It extends coding agents with a SQL compiler, a validator, an
event-time simulator, and skills. The compiler turns SQL-like scripts (SQRL) into complete data pipelines
that integrate Kafka, Flink, PostgreSQL, Iceberg, GraphQL APIs, and LLM tooling. Built with Java 17 and Maven.

## External Dependencies

The following repositories contain additional runtime components:
* [Flink SQL Runner](https://github.com/DataSQRL/flink-sql-runner): Runs the Flink compiled plan and provides additional utilities for Flink
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
```

### Testing Commands
```bash
# Unit tests only
mvn test

# Integration tests
mvn verify

# Test specific module
mvn test -pl sqrl-planner

# Test specific test method in module
mvn test -pl sqrl-tools/sqrl-config -Dtest=TestClassName#testMethodName

# Container tests (requires Docker images to be built)
mvn -B install -DonlyContainerE2E -pl :sqrl-testing-container -Dit.test=TestClassName

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

## Module Overview

Multi-module Maven project (versions live in the root `pom.xml` properties, not here):

- **sqrl-planner/** - Compiler core: parses SQRL scripts, builds and optimizes the computation DAG, and produces deployment artifacts. Built on Apache Calcite and Flink's parser.
- **sqrl-cli/** - CLI (`com.datasqrl.cli.DatasqrlCli`) with the `init`, `add-func`, `compile`, `test`, `run`, and `exec` commands, plus the packager/preprocessors (incl. JBang UDFs) and local process management for `run`/`test`.
- **sqrl-discovery/** - Automatic schema discovery for data files.
- **sqrl-deployment-model/** - Shared model classes for the compiled deployment plan (Flink, JDBC, Kafka).
- **sqrl-server/** - GraphQL/REST/MCP API server:
  - `sqrl-server-core/` - Core interfaces and models (GraphQL schema, execution coordinates)
  - `sqrl-server-vertx-base/` - Vert.x implementation with database clients, auth, and Kafka integration
  - `sqrl-server-vertx/` - Standalone server deployment (`com.datasqrl.server.SqrlLauncher`)
- **sqrl-testing/** - Integration and end-to-end tests:
  - `sqrl-testing-integration/` - Compiler and pipeline integration tests, including snapshot tests
  - `sqrl-testing-container/` - Docker image end-to-end tests
- **agent/** - DataSQRL data engineering agent image and its skills
- **documentation/** - User documentation site

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

## Pull Request Titles

PR titles are enforced by CI (`.github/workflows/lint-pr-title.yml`) using the Conventional Commits format. Titles must start with one of the following prefixes:

- `feat` – new feature
- `fix` – bug fix
- `chore` – maintenance / housekeeping
- `test` – test additions or changes
- `docs` – documentation
- `refactor` – code refactoring
- `ci` – CI/CD changes
- `build` – build system changes
- `perf` – performance improvements
- `revert` – reverting a previous change

A `!` after the type denotes a breaking change (e.g., `feat!: Remove legacy auth middleware`).

**Example**: `feat: Add support for Iceberg sink`

Note: The linter skips Dependabot PRs automatically.

## Testing Philosophy

- **Integration Testing**: Uses Testcontainers for PostgreSQL, Kafka, and other services
- **Snapshot Testing**: Ensures consistent output across builds
- **End-to-End Testing**: Full pipeline testing with real services
- **Coverage Requirement**: Minimum 70% instruction coverage with JaCoCo
- **Test Naming**: All new test methods must follow the `given_when_then` pattern (e.g., `givenValidConfig_whenParseConfiguration_thenReturnsExpectedResult`)
- **Test Assertions**: Use AssertJ (`org.assertj.core.api.Assertions`) for all test assertions. Avoid JUnit's `org.junit.jupiter.api.Assertions` in favor of AssertJ's more fluent and readable API

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

Snapshot files are located in `sqrl-testing/sqrl-testing-integration/src/test/resources/snapshots/com/datasqrl/` and contain expected outputs for integration tests. These `.txt` files capture the complete compiled output of SQRL scripts, including:

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

## Server Runtime Model
The server loads the compiler-generated `server-model.json` at startup; no SQL is generated at runtime, so GraphQL schema changes require recompiling.

## JBang UDF Files

JBang-based user-defined functions (UDFs) are detected by the `JBangPreprocessor` using a shebang-based opt-in mechanism:

- JBang UDF files **must** start with `///usr/bin/env jbang "$0" "$@" ; exit $?` as their first line
- `.java` files without the shebang are ignored by the preprocessor, even if they extend a Flink UDF class
- Flink dependencies are **not** on JBang's classpath: every JBang UDF must declare `//DEPS org.apache.flink:flink-table-common:<flink.version>` (see `documentation/docs/functions.md`)
