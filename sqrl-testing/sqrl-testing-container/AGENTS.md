# Container Testing

Container tests in `sqrl-testing-container` validate the end-to-end functionality of DataSQRL Docker images:

- **Purpose**: Test the complete Docker image deployment including compilation and server startup
- **Requirements**: Docker must be running and DataSQRL images must be built (`datasqrl/cmd:local`, `datasqrl/sqrl-server:local`)
- **Test Structure**: Tests use JUnit extension `SqrlContainerExtension` and `PostgresContainerExtension` which provide container management utilities
- **Available Endpoints**: 
  - `/graphql` - Main GraphQL API endpoint
  - `/health` - Health check endpoint (returns 204 No Content when healthy)
  - `/metrics` - Prometheus metrics endpoint (availability depends on configuration)
- **Common Patterns**: Compile SQRL script → Start server container → Execute HTTP requests → Validate responses
- **Test Data**: Uses test cases from `sqrl-testing-integration/src/test/resources/usecases/`
