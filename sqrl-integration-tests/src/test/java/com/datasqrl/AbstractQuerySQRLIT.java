/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanExecutor;
import com.datasqrl.engine.database.relational.JDBCPhysicalPlan;
import com.datasqrl.graphql.GraphQLServer;
import com.datasqrl.graphql.inference.AbstractSchemaInferenceModelTest;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.util.ReplaceGraphqlQueries;
import com.datasqrl.io.jdbc.JdbcDataSystemConnectorConfig;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.local.generate.Namespace;
import com.datasqrl.plan.queries.APIQuery;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.pgclient.impl.PgPoolOptions;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class AbstractQuerySQRLIT extends AbstractPhysicalSQRLIT {

  protected Vertx vertx;
  protected VertxTestContext vertxContext;

  ObjectMapper mapper = new ObjectMapper();
  private int port;

  @SneakyThrows
  @AfterEach
  public void stop() {
    if (vertx != null) {
      vertx.close().toCompletionStage().toCompletableFuture().get();
    }
  }

  @SneakyThrows
  protected void validateSchemaAndQueries(String script, String schema,
      Map<String, String> queries) {

    Namespace ns = plan(script);

    AbstractSchemaInferenceModelTest t = new AbstractSchemaInferenceModelTest(ns);
    Pair<RootGraphqlModel, List<APIQuery>> modelAndQueries = t
        .getModelAndQueries(planner, schema);

    PhysicalDAGPlan dag = physicalPlanner.planDag(ns, modelAndQueries.getRight());

    PhysicalPlan physicalPlan = physicalPlanner.createPhysicalPlan(dag);

    RootGraphqlModel model = modelAndQueries.getKey();
    ReplaceGraphqlQueries replaceGraphqlQueries = new ReplaceGraphqlQueries(
        physicalPlan.getDatabaseQueries());

    model.accept(replaceGraphqlQueries, null);

    snapshot.addContent(
        physicalPlan.getPlans(JDBCPhysicalPlan.class).findFirst().get().getDdlStatements().stream()
            .map(ddl -> ddl.toSql())
            .sorted().collect(Collectors.joining(System.lineSeparator())), "database");

    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
    executor.execute(physicalPlan);


    SqlClient client;

    if (jdbc.getConfig().getDialect().equalsIgnoreCase("postgres")) {
      client = PgPool.client(vertx, toPgOptions(jdbc.getConfig()),
          new PgPoolOptions(new PoolOptions()));
    } else {
      client = JDBCPool.pool(
          vertx,
          toJdbcConfig(jdbc.getConfig()),
          new PoolOptions());

    }
    CountDownLatch countDownLatch = new CountDownLatch(1);


    this.port = getPort(8888);
    GraphQLServer server = new GraphQLServer(
        model, port, client);
    vertx.deployVerticle(server, c->countDownLatch.countDown());
    countDownLatch.await(10, TimeUnit.SECONDS);
    if (countDownLatch.getCount() != 0) {
      throw new RuntimeException();
    }

    for (Map.Entry<String, String> query : queries.entrySet()) {
      HttpResponse<String> response = testQuery(query.getValue());
      String httpQuery = prettyPrint(response.body());

      snapshot.addContent(httpQuery, "query", query.getKey());
    }
    snapshot.createOrValidate();
    vertxContext.completeNow();
  }

  @SneakyThrows
  private static int getPort(int port) {
    int attempts = 10;
    while(attempts-- != 0) {
      try (ServerSocket serverSocket = new ServerSocket(port)) {
        boolean isPortBound = serverSocket.isBound();
        if (isPortBound) {
          return port;
        }
      } catch (Exception e) {
        port++;
      }
    }
    return -1;
  }

  private JDBCConnectOptions toJdbcConfig(JdbcDataSystemConnectorConfig config) {
    JDBCConnectOptions options = new JDBCConnectOptions()
        .setJdbcUrl(jdbc.getConfig().getDbURL())
        .setDatabase(jdbc.getConfig().getDatabase());

    Optional.ofNullable(config.getUser()).map(options::setUser);
    Optional.ofNullable(config.getPassword()).map(options::setPassword);
    return options;
  }

  @SneakyThrows
  private String prettyPrintObj(Object body) {
    return mapper.writerWithDefaultPrettyPrinter()
        .writeValueAsString(body);
  }

  @SneakyThrows
  private String prettyPrint(String body) {
    return mapper.writerWithDefaultPrettyPrinter()
        .writeValueAsString(mapper.readTree(body));
  }

  private PgConnectOptions toPgOptions(JdbcDataSystemConnectorConfig jdbcConf) {
    PgConnectOptions options = new PgConnectOptions();
    options.setDatabase(jdbcConf.getDatabase());
    options.setHost(jdbcConf.getHost());
    options.setPort(jdbcConf.getPort());
    options.setUser(jdbcConf.getUser());
    options.setPassword(jdbcConf.getPassword());
    options.setCachePreparedStatements(true);
    options.setPipeliningLimit(100_000);
    return options;
  }

  @SneakyThrows
  public HttpResponse<String> testQuery(String query) {
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request = HttpRequest.newBuilder()
        .POST(HttpRequest.BodyPublishers.ofString(mapper
            .writeValueAsString(
                Map.of("query", query))))
        .uri(URI.create("http://localhost:" + port + "/graphql"))
        .build();
    return client.send(request, BodyHandlers.ofString());
  }
}
