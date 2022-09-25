package ai.datasqrl.graphql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.IntegrationTestSettings.DatabaseEngine;
import ai.datasqrl.graphql.server.Model.ArgumentLookupCoords;
import ai.datasqrl.graphql.server.Model.ArgumentPgParameter;
import ai.datasqrl.graphql.server.Model.ArgumentSet;
import ai.datasqrl.graphql.server.Model.FixedArgument;
import ai.datasqrl.graphql.server.Model.PgQuery;
import ai.datasqrl.graphql.server.Model.Root;
import ai.datasqrl.graphql.server.Model.SourcePgParameter;
import ai.datasqrl.graphql.server.Model.StringSchema;
import ai.datasqrl.graphql.server.Model.VariableArgument;
import ai.datasqrl.graphql.server.VertxGraphQLBuilder;
import ai.datasqrl.graphql.server.VertxGraphQLBuilder.VertxContext;
import ai.datasqrl.util.JDBCTestDatabase;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.impl.SqlClientInternal;
import java.sql.Connection;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.PostgreSQLContainer;

@ExtendWith(VertxExtension.class)
class VertxGraphQLBuilderTest extends AbstractSQRLIT {
  static Root root = Root.builder()
      .schema(StringSchema.builder().schema(""
          + "type Query { "
          + "  customer(sort: SortKey): Customer "
          + "  customer2(customerid: Int = 2): Customer "
          + "} "
          + "type Customer {"
          + "  customerid: Int "
          + "  sameCustomer: Customer"
          + "} "
          + "input SortKey {customerid: Direction} "
          + "enum Direction {DESC, ASC}").build())
      .coord(ArgumentLookupCoords.builder()
          .parentType("Query")
          .fieldName("customer")
          .match(ArgumentSet.builder()
              .argument(FixedArgument.builder()
                  .path("sort.customerid")
                  .value("DESC")
                  .build())
              .query(PgQuery.builder()
                  .sql("SELECT customerid FROM Customer ORDER BY customerid DESC")
                  .build())
              .build())
          .match(ArgumentSet.builder()
              .argument(FixedArgument.builder()
                  .path("sort.customerid")
                  .value("ASC")
                  .build())
              .query(PgQuery.builder()
                  .sql("SELECT customerid FROM Customer ORDER BY customerid ASC")
                  .build())
              .build())
          .build())

      .coord(ArgumentLookupCoords.builder()
          .parentType("Query")
          .fieldName("customer2")
          .match(ArgumentSet.builder()
              .argument(VariableArgument.builder()
                  .path("customerid")
                  .build())
              .query(PgQuery.builder()
                  .sql("SELECT customerid FROM Customer WHERE customerid = $1")
                  .parameter(ArgumentPgParameter.builder()
                      .path("customerid")
                      .build())
                  .build())
              .build())
          .build())
      .coord(ArgumentLookupCoords.builder()
          .parentType("Customer")
          .fieldName("sameCustomer")
          .match(ArgumentSet.builder()
              .query(PgQuery.builder()
                  .sql("SELECT customerid FROM Customer WHERE customerid = $1")
                  .parameter(SourcePgParameter.builder()
                      .key("customerid")
                      .build())
                  .build())
              .build())
          .build())
      .build();
  private SqlClientInternal client;

  @SneakyThrows
  @BeforeEach
  public void init(Vertx vertx, VertxTestContext testContext) {
    initialize(IntegrationTestSettings.builder()
        .database(DatabaseEngine.POSTGRES)
        .build());

    JDBCTestDatabase testDatabase = (JDBCTestDatabase) this.database;

    Connection con = testDatabase.getJdbcConfiguration()
        .getDatabase(testDatabase.getPostgreSQLContainer().getDatabaseName())
        .getConnection();

    con.createStatement()
            .execute("CREATE TABLE Customer (customerid INTEGER)");
    con.createStatement()
        .execute("INSERT INTO Customer(customerid) VALUES (1);");
    con.createStatement()
        .execute("INSERT INTO Customer(customerid) VALUES (2);");
    con.close();

    PgConnectOptions options = new PgConnectOptions();
    options.setDatabase(testDatabase.getPostgreSQLContainer().getDatabaseName());
    options.setHost(testDatabase.getPostgreSQLContainer().getHost());
    options.setPort(testDatabase.getPostgreSQLContainer().getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT));
    options.setUser(testDatabase.getPostgreSQLContainer().getUsername());
    options.setPassword(testDatabase.getPostgreSQLContainer().getPassword());

    options.setCachePreparedStatements(true);
    options.setPipeliningLimit(100_000);

    PgConnection.connect(vertx, options).flatMap(conn -> {
      client = (SqlClientInternal) conn;
      return Future.succeededFuture();
    }).onComplete(testContext.succeedingThenComplete())
      .onFailure(f -> testContext.failNow(f));
  }

  @AfterEach
  public void after() {
    client.close();
  }

  @SneakyThrows
  @Test
  public void test() {
    GraphQL graphQL = root.accept(
        new VertxGraphQLBuilder(),
        new VertxContext(client));

    ExecutionResult result = graphQL.execute("{\n"
        + "  casc: customer(sort: {customerid: ASC}) {\n"
        + "    customerid\n"
        + "    sameCustomer {\n"
        + "      customerid\n"
        + "    }\n"
        + "  }\n"
        + "  cdesc: customer(sort: {customerid: DESC}) {\n"
        + "    customerid\n"
        + "    sameCustomer {\n"
        + "      customerid\n"
        + "    }\n"
        + "  }\n"
        + "  customer2(customerid: 2) {\n"
        + "    customerid\n"
        + "  }\n"
        + "}");

    if (result.getErrors() != null && !result.getErrors().isEmpty()) {
      fail(result.getErrors().stream()
          .map(GraphQLError::getMessage)
          .collect(Collectors.joining(",")));
    }
    String value = new ObjectMapper().writeValueAsString(result.getData());
    assertEquals(
        "{\"casc\":{\"customerid\":1,\"sameCustomer\":{\"customerid\":1}},\"cdesc\":{\"customerid\":2,\"sameCustomer\":{\"customerid\":2}},\"customer2\":{\"customerid\":2}}",
        value);

  }
}