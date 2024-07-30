/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentLookupCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentParameter;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentSet;
import com.datasqrl.graphql.server.RootGraphqlModel.CalciteQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.FieldLookupCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.JdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.SourceParameter;
import com.datasqrl.graphql.server.RootGraphqlModel.StringSchema;
import com.datasqrl.graphql.server.RootGraphqlModel.VariableArgument;
import com.datasqrl.graphql.server.GraphQLEngineBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.ExperimentalApi;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.execution.AsyncExecutionStrategy;
import graphql.execution.instrumentation.ChainedInstrumentation;
import graphql.incremental.DeferPayload;
import graphql.incremental.DelayedIncrementalPartialResult;
import graphql.incremental.IncrementalExecutionResult;
import io.vertx.core.Vertx;
import io.vertx.ext.web.handler.graphql.instrumentation.JsonObjectAdapter;
import io.vertx.ext.web.handler.graphql.instrumentation.VertxFutureAdapter;
import io.vertx.junit5.VertxExtension;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
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
      new PostgreSQLContainer(DockerImageName.parse("ankane/pgvector:v0.5.0")
          .asCompatibleSubstituteFor("postgres"))
          .withDatabaseName("foo")
          .withUsername("foo")
          .withPassword("secret")
          .withDatabaseName("datasqrl");

  static RootGraphqlModel root = RootGraphqlModel.builder()
      .schema(StringSchema.builder().schema(""
//          Directives.DeferDirective
           + "directive @defer(label: String = \"No longer supported\") on INLINE_FRAGMENT\n"
          + " "
          + "type Query { "
          + "  customer: [Customer] "
          + "} "
          + "type Customer {"
          + "  customerid: Int "
          + "}"
         ).build())
      .coord(FieldLookupCoords.builder()
          .parentType("Customer")
          .fieldName("customerid")
          .columnName("customerid")
          .build())
      .coord(ArgumentLookupCoords.builder()
          .parentType("Query")
          .fieldName("customer")
          .match(ArgumentSet.builder()
              .query(
                  new CalciteQuery("select STREAM *\n"
                      + "from \"flink\".\"sales_fact_1997\"", List.of())
              )
              .build())

          .build())

      .coord(ArgumentLookupCoords.builder()
          .parentType("Query")
          .fieldName("customer2")
          .match(ArgumentSet.builder()
              .argument(VariableArgument.builder()
                  .path("customerid")
                  .build())
              .query(new JdbcQuery("SELECT customerid FROM Customer WHERE customerid = $1", List.of(ArgumentParameter.builder()
                      .path("customerid")
                      .build())))
              .build())
          .build())
      .coord(ArgumentLookupCoords.builder()
          .parentType("Customer")
          .fieldName("sameCustomer")
          .match(ArgumentSet.builder()
              .query(new JdbcQuery("SELECT customerid FROM Customer WHERE customerid = $1", List.of(SourceParameter.builder()
                      .key("customerid")
                      .build())))
              .build())
          .build())
      .build();
  private PgPool client;
  private Vertx vertx;

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
    this.vertx = vertx;
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
  @Disabled//requires sql gateway
  public void test() {
    GraphQL graphQL = root.accept(
        new GraphQLEngineBuilder(),
        new VertxContext(vertx, new VertxJdbcClient(client), GraphQLServer.getCalciteClient(vertx), Map.of(), Map.of(), NameCanonicalizer.SYSTEM))
        .instrumentation(new ChainedInstrumentation(
            new JsonObjectAdapter(), VertxFutureAdapter.create()))
        .queryExecutionStrategy(new AsyncExecutionStrategy())
        .build();
    long time;
    for (int i = 0; i < 2; i++) {
      time = System.currentTimeMillis();
      CountDownLatch latch = new CountDownLatch(1);

      ExecutionInput input = ExecutionInput.newExecutionInput().query("query {\n"
          + "  ... @defer { customer { customerid } }"
          + " }"
      ).graphQLContext(
          Map.of(ExperimentalApi.ENABLE_INCREMENTAL_SUPPORT, true)).build();

      ExecutionResult initialResult = graphQL
          .executeAsync(input)
          .get();

      IncrementalExecutionResult result = (IncrementalExecutionResult) initialResult;

      Publisher<DelayedIncrementalPartialResult> delayedIncrementalResults = result
          .getIncrementalItemPublisher();

      delayedIncrementalResults.subscribe(new Subscriber<>() {

        Subscription subscription;

        int calls = 0;

        @Override
        public void onSubscribe(Subscription s) {
          subscription = s;
          //
          // how many you request is up to you
          subscription.request(1);
        }

        @Override
        public void onNext(DelayedIncrementalPartialResult executionResult) {
          //
          // as each deferred result arrives, send it to where it needs to go
          //
          executionResult.getIncremental().stream().map(i -> (DeferPayload) i)
              .map(d -> d.getData())
              .forEach(System.out::println);
          calls++;
          System.out.println(executionResult.hasNext());
          latch.countDown();
//        subscription.request(10);
        }

        @Override
        public void onError(Throwable t) {
          fail(t);
          t.printStackTrace();
        }

        @Override
        public void onComplete() {
          assertEquals(1, calls);
        }
      });
      latch.await();

      if (result.getErrors() != null && !result.getErrors().isEmpty()) {
        fail(result.getErrors().stream()
            .map(GraphQLError::getMessage)
            .collect(Collectors.joining(",")));
      }
      String value = new ObjectMapper().writeValueAsString(result.getData());
      System.out.println(value);
      assertEquals(
          "{}",
          value);
      System.out.println(System.currentTimeMillis() - time);
    }
  }
}