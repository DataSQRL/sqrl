/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/// *
// * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
// */
// package com.datasqrl.graphql;
//
// import static org.junit.jupiter.api.Assertions.assertEquals;
// import static org.junit.jupiter.api.Assertions.fail;
//
// import com.datasqrl.graphql.server.RootGraphqlModel;
// import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentLookupCoords;
// import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentParameter;
// import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentSet;
// import com.datasqrl.graphql.server.RootGraphqlModel.FixedArgument;
// import com.datasqrl.graphql.server.RootGraphqlModel.JdbcQuery;
// import com.datasqrl.graphql.server.RootGraphqlModel.SourceParameter;
// import com.datasqrl.graphql.server.RootGraphqlModel.StringSchema;
// import com.datasqrl.graphql.server.RootGraphqlModel.VariableArgument;
// import com.datasqrl.graphql.server.GraphQLEngineBuilder;
// import com.fasterxml.jackson.databind.ObjectMapper;
// import graphql.ExecutionResult;
// import graphql.GraphQL;
// import graphql.GraphQLError;
// import io.vertx.core.Vertx;
// import io.vertx.junit5.VertxExtension;
// import io.vertx.pgclient.PgConnectOptions;
// import io.vertx.pgclient.PgPool;
// import io.vertx.sqlclient.PoolOptions;
// import io.vertx.sqlclient.SqlConnection;
// import java.util.List;
// import java.util.Map;
// import java.util.stream.Collectors;
// import lombok.SneakyThrows;
// import org.junit.jupiter.api.AfterEach;
// import org.junit.jupiter.api.BeforeEach;
// import org.junit.jupiter.api.Test;
// import org.junit.jupiter.api.extension.ExtendWith;
// import org.testcontainers.containers.PostgreSQLContainer;
// import org.testcontainers.junit.jupiter.Container;
// import org.testcontainers.junit.jupiter.Testcontainers;
// import org.testcontainers.utility.DockerImageName;
//
// @ExtendWith(VertxExtension.class)
// @Testcontainers
// class VertxGraphQLBuilderTest {
//
//  // will be started before and stopped after each test method
//  @Container
//  private PostgreSQLContainer testDatabase =
//      new PostgreSQLContainer(DockerImageName.parse("ankane/pgvector:v0.5.0")
//          .asCompatibleSubstituteFor("postgres"))
//          .withDatabaseName("foo")
//          .withUsername("foo")
//          .withPassword("secret")
//          .withDatabaseName("datasqrl");
//
//  static RootGraphqlModel root = RootGraphqlModel.builder()
//      .schema(StringSchema.builder().schema(""
//          + "type Query { "
//          + "  customer2(customerid: Int = 2): Customer "
//          + "} "
//          + "type Customer {"
//          + "  customerid: Int "
//          + "  sameCustomer: Customer"
//          + "}").build())
//      .coord(ArgumentLookupCoords.builder()
//          .parentType("Query")
//          .fieldName("customer")
//          .match(ArgumentSet.builder()
//              .argument(FixedArgument.builder()
//                  .path("sort.customerid")
//                  .value("DESC")
//                  .build())
//              .query(new JdbcQuery("SELECT customerid FROM Customer ORDER BY customerid DESC",
// List.of()))
//              .build())
//          .match(ArgumentSet.builder()
//              .argument(FixedArgument.builder()
//                  .path("sort.customerid")
//                  .value("ASC")
//                  .build())
//              .query(new JdbcQuery("SELECT customerid FROM Customer ORDER BY customerid ASC",
// List.of()))
//              .build())
//          .build())
//
//      .coord(ArgumentLookupCoords.builder()
//          .parentType("Query")
//          .fieldName("customer2")
//          .match(ArgumentSet.builder()
//              .argument(VariableArgument.builder()
//                  .path("customerid")
//                  .build())
//              .query(new JdbcQuery("SELECT customerid FROM Customer WHERE customerid = $1",
// List.of(ArgumentParameter.builder()
//                      .path("customerid")
//                      .build())))
//              .build())
//          .build())
//      .coord(ArgumentLookupCoords.builder()
//          .parentType("Customer")
//          .fieldName("sameCustomer")
//          .match(ArgumentSet.builder()
//              .query(new JdbcQuery("SELECT customerid FROM Customer WHERE customerid = $1",
// List.of(SourceParameter.builder()
//                      .key("customerid")
//                      .build())))
//              .build())
//          .build())
//      .build();
//  private PgPool client;
//
//  @SneakyThrows
//  @BeforeEach
//  public void init(Vertx vertx) {
//    PgConnectOptions options = new PgConnectOptions();
//    options.setDatabase(testDatabase.getDatabaseName());
//    options.setHost(testDatabase.getHost());
//    options.setPort(testDatabase.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT));
//    options.setUser(testDatabase.getUsername());
//    options.setPassword(testDatabase.getPassword());
//
//    options.setCachePreparedStatements(true);
//    options.setPipeliningLimit(100_000);
//
//    PgPool client = PgPool.pool(vertx, options, new PoolOptions());
//    this.client = client;
//
//    SqlConnection con =
//        client.getConnection().toCompletionStage().toCompletableFuture().get();
//    con.preparedQuery("CREATE TABLE Customer (customerid INTEGER)")
//        .execute().toCompletionStage().toCompletableFuture().get();
//    con.preparedQuery("INSERT INTO Customer(customerid) VALUES (1)")
//        .execute().toCompletionStage().toCompletableFuture().get();
//    con.preparedQuery("INSERT INTO Customer(customerid) VALUES (2)")
//        .execute().toCompletionStage().toCompletableFuture().get();
//
//  }
//
//  @AfterEach
//  public void after() {
//    client.close();
//  }
//
//  @SneakyThrows
//  @Test
//  public void test() {
//    GraphQL graphQL = root.accept(
//        new GraphQLEngineBuilder(),
//        new VertxContext(new VertxJdbcClient(client), Map.of(), Map.of(),
// NameCanonicalizer.SYSTEM));
//    ExecutionResult result = graphQL.execute("{\n"
//        + "  customer2(customerid: 2) {\n"
//        + "    customerid\n"
//        + "  }\n"
//        + "}");
//
//    if (result.getErrors() != null && !result.getErrors().isEmpty()) {
//      fail(result.getErrors().stream()
//          .map(GraphQLError::getMessage)
//          .collect(Collectors.joining(",")));
//    }
//    String value = new ObjectMapper().writeValueAsString(result.getData());
//    System.out.println(value);
//    assertEquals(
//        "{\"customer2\":{\"customerid\":2}}",
//        value);
//  }
// }
