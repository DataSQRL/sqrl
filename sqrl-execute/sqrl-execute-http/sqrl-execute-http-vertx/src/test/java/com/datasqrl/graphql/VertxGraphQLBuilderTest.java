/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.graphql.server.Model.ArgumentLookupQueryCoords;
import com.datasqrl.graphql.server.Model.ArgumentParameter;
import com.datasqrl.graphql.server.Model.ArgumentSet;
import com.datasqrl.graphql.server.Model.FixedArgument;
import com.datasqrl.graphql.server.Model.JdbcQuery;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.server.Model.SourceParameter;
import com.datasqrl.graphql.server.Model.StringSchema;
import com.datasqrl.graphql.server.Model.VariableArgument;
import com.datasqrl.graphql.server.BuildGraphQLEngine;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnection;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@ExtendWith(VertxExtension.class)
@Testcontainers
class VertxGraphQLBuilderTest {

  // will be started before and stopped after each test method
  @Container
  private PostgreSQLContainer testDatabase =
      new PostgreSQLContainer(DockerImageName.parse("postgres:14.2"))
          .withDatabaseName("foo")
          .withUsername("foo")
          .withPassword("secret")
          .withDatabaseName("datasqrl");

  static RootGraphqlModel root = RootGraphqlModel.builder()
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
      .coord(ArgumentLookupQueryCoords.builder()
          .parentType("Query")
          .fieldName("customer")
          .match(ArgumentSet.builder()
              .argument(FixedArgument.builder()
                  .path("sort.customerid")
                  .value("DESC")
                  .build())
              .query(JdbcQuery.builder()
                  .sql("SELECT customerid FROM Customer ORDER BY customerid DESC")
                  .build())
              .build())
          .match(ArgumentSet.builder()
              .argument(FixedArgument.builder()
                  .path("sort.customerid")
                  .value("ASC")
                  .build())
              .query(JdbcQuery.builder()
                  .sql("SELECT customerid FROM Customer ORDER BY customerid ASC")
                  .build())
              .build())
          .build())

      .coord(ArgumentLookupQueryCoords.builder()
          .parentType("Query")
          .fieldName("customer2")
          .match(ArgumentSet.builder()
              .argument(VariableArgument.builder()
                  .path("customerid")
                  .build())
              .query(JdbcQuery.builder()
                  .sql("SELECT customerid FROM Customer WHERE customerid = $1")
                  .parameter(ArgumentParameter.builder()
                      .path("customerid")
                      .build())
                  .build())
              .build())
          .build())
      .coord(ArgumentLookupQueryCoords.builder()
          .parentType("Customer")
          .fieldName("sameCustomer")
          .match(ArgumentSet.builder()
              .query(JdbcQuery.builder()
                  .sql("SELECT customerid FROM Customer WHERE customerid = $1")
                  .parameter(SourceParameter.builder()
                      .key("customerid")
                      .build())
                  .build())
              .build())
          .build())
      .build();
  private PgPool client;

  @SneakyThrows
  @BeforeEach
  public void init(Vertx vertx) {
    PgConnectOptions options = new PgConnectOptions();
    options.setDatabase(testDatabase.getDatabaseName());
    options.setHost(testDatabase.getHost());
    options.setPort(testDatabase.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT));
    options.setUser(testDatabase.getUsername());
    options.setPassword(testDatabase.getPassword());

    options.setCachePreparedStatements(true);
    options.setPipeliningLimit(100_000);

    PgPool client = PgPool.pool(vertx, options, new PoolOptions());
    this.client = client;

    SqlConnection con =
        client.getConnection().toCompletionStage().toCompletableFuture().get();
    con.preparedQuery("CREATE TABLE Customer (customerid INTEGER)")
        .execute().toCompletionStage().toCompletableFuture().get();
    con.preparedQuery("INSERT INTO Customer(customerid) VALUES (1)")
        .execute().toCompletionStage().toCompletableFuture().get();
    con.preparedQuery("INSERT INTO Customer(customerid) VALUES (2)")
        .execute().toCompletionStage().toCompletableFuture().get();

  }

  @AfterEach
  public void after() {
    client.close();
  }

  @SneakyThrows
  @Test
  public void test() {
    GraphQL graphQL = root.accept(
        new BuildGraphQLEngine(),
        new VertxContext(new VertxJdbcClient(client), Map.of(), Map.of()));
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