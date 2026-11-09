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
package com.datasqrl.server.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.datasqrl.server.PaginationType;
import com.datasqrl.server.VertxServerContext;
import com.datasqrl.server.graphql.CustomScalars;
import com.datasqrl.server.graphql.GraphQLEngineBuilder;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.graphql.RootGraphQLModel.ArgumentLookupQueryCoords;
import com.datasqrl.server.graphql.RootGraphQLModel.FieldLookupQueryCoords;
import com.datasqrl.server.graphql.RootGraphQLModel.QueryWithArguments;
import com.datasqrl.server.graphql.RootGraphQLModel.SqlQuery;
import com.datasqrl.server.graphql.RootGraphQLModel.StringSchema;
import com.datasqrl.server.jdbc.DatabaseType;
import com.datasqrl.server.jdbc.VertxJdbcClient;
import com.datasqrl.server.jdbc.VertxParamArgumentTypeMapper;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.GraphQL;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.internal.threadchecker.BlockedThreadEvent;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandler;
import io.vertx.pgclient.PgBuilder;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Streams a 250k row result from PostgreSQL through the HTTP handler chain the server uses, and
 * compares it with the regular GraphQL handler serving the same engine.
 */
@Testcontainers
class StreamingQueryIT {

  private static final int ROWS = 250_000;
  private static final int FETCH_SIZE = 1000;
  private static final int FAILING_ROW = 150_000;
  private static final long MAX_EVENT_LOOP_MILLIS = 1000;
  private static final String ORDER_FIELDS =
      "id customer amount total quantity paid createdAt day payload tags";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Container
  private static final PostgreSQLContainer postgresContainer =
      new PostgreSQLContainer(DockerImageName.parse("postgres:16"))
          .withDatabaseName("datasqrl")
          .withUsername("foo")
          .withPassword("secret");

  private static final List<BlockedThreadEvent> blockedEventLoops = new CopyOnWriteArrayList<>();
  private static Vertx vertx;
  private static Pool pool;
  private static HttpServer server;
  private static HttpClient httpClient;

  @BeforeAll
  static void startServer() {
    vertx =
        Vertx.vertx(
            new VertxOptions()
                .setMaxEventLoopExecuteTime(MAX_EVENT_LOOP_MILLIS)
                .setMaxEventLoopExecuteTimeUnit(TimeUnit.MILLISECONDS)
                .setBlockedThreadCheckInterval(100)
                .setBlockedThreadCheckIntervalUnit(TimeUnit.MILLISECONDS));
    ((VertxInternal) vertx).blockedThreadChecker().setThreadBlockedHandler(blockedEventLoops::add);

    var options =
        new PgConnectOptions()
            .setDatabase(postgresContainer.getDatabaseName())
            .setHost(postgresContainer.getHost())
            .setPort(postgresContainer.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT))
            .setUser(postgresContainer.getUsername())
            .setPassword(postgresContainer.getPassword());
    pool =
        PgBuilder.pool()
            .with(new PoolOptions().setMaxSize(4))
            .connectingTo(options)
            .using(vertx)
            .build();
    join(pool.query(CREATE_ORDERS).execute());
    join(pool.query(INSERT_ORDERS).execute());

    var engine = engine(pool);
    var streaming =
        new StreamingGraphQLHandler(
            engine,
            new StreamEligibility(engine.getGraphQLSchema(), model().getQueries()),
            pool,
            FETCH_SIZE);
    var router = Router.router(vertx);
    router.route().handler(BodyHandler.create());
    router.route("/graphql").handler(streaming).handler(GraphQLHandler.create(engine));
    router.route("/plain").handler(GraphQLHandler.create(engine));
    server = join(vertx.createHttpServer().requestHandler(router).listen(0));
    httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
  }

  @AfterAll
  static void stopServer() {
    join(server.close());
    join(pool.close());
    join(vertx.close());
  }

  @BeforeEach
  void clearBlockedEventLoops() {
    blockedEventLoops.clear();
  }

  @Test
  void givenUnboundedListQuery_whenStreamed_thenEveryRowArrivesInChunksWithoutBlockingEventLoop() {
    var response = post("/graphql", "{ orders { " + ORDER_FIELDS + " } }");

    assertThat(response.headers().firstValue("transfer-encoding")).contains("chunked");
    var ids = readOrderIds(response.body());
    assertThat(ids.count()).isEqualTo(ROWS);
    assertThat(ids.inOrder()).isTrue();
    assertThat(blockedEventLoops).isEmpty();
  }

  @Test
  void givenSameRequest_whenStreamedAndServedRegularly_thenBodiesAreIdentical() {
    var query = "{ orders(limit: 5000, offset: 100) { renamed: " + ORDER_FIELDS + " } }";

    var streamed = postForString("/graphql", query);
    var regular = postForString("/plain", query);

    assertThat(streamed.headers().firstValue("transfer-encoding")).contains("chunked");
    assertThat(regular.headers().firstValue("transfer-encoding")).isEmpty();
    assertThat(streamed.body()).isEqualTo(regular.body());
    assertThat(new JsonObject(streamed.body()).getJsonObject("data").getJsonArray("orders"))
        .hasSize(5000);
  }

  @Test
  void givenIneligibleRequest_whenPosted_thenRegularHandlerServesIt() {
    var query = "{ orders(limit: 3) { id } __typename }";

    var streamed = postForString("/graphql", query);

    assertThat(streamed.headers().firstValue("transfer-encoding")).isEmpty();
    assertThat(streamed.body()).isEqualTo(postForString("/plain", query).body());
  }

  @Test
  void givenInvalidRequest_whenPosted_thenValidationErrorMatchesRegularHandler() {
    var query = "{ orders(limit: \"many\") { id } }";

    assertThat(postForString("/graphql", query).body())
        .isEqualTo(postForString("/plain", query).body())
        .contains("\"errors\"");
  }

  @Test
  @SneakyThrows
  void givenQueryFailingAfterFirstChunk_whenStreamed_thenRowsAreTerminatedAndErrorAppended() {
    var body = postForString("/graphql", "{ failingOrders { id ratio } }").body();

    var response = new JsonObject(body);
    var rows = response.getJsonObject("data").getJsonArray("failingOrders");
    assertThat(rows.size()).isPositive().isLessThan(FAILING_ROW);
    assertThat(response.getJsonArray("errors").getJsonObject(0).getString("message"))
        .startsWith("Exception while fetching data (/failingOrders) :")
        .contains("division by zero");
    awaitNoOpenTransactions();
  }

  @Test
  @SneakyThrows
  void givenClientDisconnectsMidStream_whenStreaming_thenCursorConnectionIsReleased() {
    var response = post("/graphql", "{ orders { " + ORDER_FIELDS + " } }");
    try (var body = response.body()) {
      assertThat(body.readNBytes(256 * 1024)).hasSize(256 * 1024);
    }

    awaitNoOpenTransactions();
    assertThat(postForString("/graphql", "{ orders(limit: 1) { id } }").body())
        .isEqualTo("{\"data\":{\"orders\":[{\"id\":1}]}}");
  }

  private static void awaitNoOpenTransactions() {
    await()
        .atMost(Duration.ofSeconds(10))
        .until(
            () ->
                join(pool.query(
                                "SELECT count(*) FROM pg_stat_activity WHERE backend_type ="
                                    + " 'client backend' AND state <> 'idle' AND pid <>"
                                    + " pg_backend_pid()")
                            .execute())
                        .iterator()
                        .next()
                        .getLong(0)
                    == 0);
  }

  @SneakyThrows
  private static OrderIds readOrderIds(InputStream body) {
    try (var parser = MAPPER.createParser(body)) {
      while (parser.nextToken() != JsonToken.START_ARRAY) {
        assertThat(parser.currentToken()).isNotNull();
      }
      var count = 0;
      var inOrder = true;
      while (parser.nextToken() == JsonToken.START_OBJECT) {
        JsonNode order = parser.readValueAsTree();
        count++;
        inOrder &= order.get("id").asInt() == count;
      }
      return new OrderIds(count, inOrder);
    }
  }

  private record OrderIds(int count, boolean inOrder) {}

  @SneakyThrows
  private static HttpResponse<InputStream> post(String path, String query) {
    return httpClient.send(request(path, query), BodyHandlers.ofInputStream());
  }

  @SneakyThrows
  private static HttpResponse<String> postForString(String path, String query) {
    return httpClient.send(request(path, query), BodyHandlers.ofString());
  }

  private static HttpRequest request(String path, String query) {
    return HttpRequest.newBuilder(URI.create("http://localhost:" + server.actualPort() + path))
        .header("Content-Type", "application/json")
        .POST(BodyPublishers.ofString(new JsonObject().put("query", query).encode()))
        .build();
  }

  private static GraphQL engine(Pool pool) {
    return model()
        .accept(
            new GraphQLEngineBuilder.Builder()
                .withExtendedScalarTypes(CustomScalars.getExtendedScalars())
                .build(),
            new VertxServerContext(
                new VertxJdbcClient(Map.of(DatabaseType.POSTGRES, pool)),
                Map.of(),
                null,
                new VertxParamArgumentTypeMapper()))
        .build();
  }

  private static RootGraphQLModel model() {
    return RootGraphQLModel.builder()
        .schema(
            StringSchema.builder()
                .schema(
                    """
                    scalar DateTime
                    scalar Date
                    scalar JSON
                    scalar Long
                    type Query {
                      orders(limit: Int, offset: Int = 0): [Order!]
                      failingOrders: [FailingOrder!]
                    }
                    type Order {
                      id: Int!
                      customer: String
                      amount: Float
                      total: Float
                      quantity: Long
                      paid: Boolean
                      createdAt: DateTime
                      day: Date
                      payload: JSON
                      tags: [String]
                    }
                    type FailingOrder {
                      id: Int!
                      ratio: Int
                    }
                    """)
                .build())
        .query(
            lookup("orders", "SELECT * FROM orders ORDER BY id", PaginationType.LIMIT_AND_OFFSET))
        .query(
            lookup(
                "failingOrders",
                "SELECT id, 1 / (" + FAILING_ROW + " - id) AS ratio FROM orders",
                PaginationType.NONE))
        .query(
            FieldLookupQueryCoords.builder()
                .parentType("Order")
                .fieldName("createdAt")
                .columnName("created_at")
                .build())
        .build();
  }

  private static ArgumentLookupQueryCoords lookup(
      String fieldName, String sql, PaginationType pagination) {
    return ArgumentLookupQueryCoords.builder()
        .parentType("Query")
        .fieldName(fieldName)
        .exec(
            QueryWithArguments.builder()
                .query(
                    new SqlQuery(
                        sql, List.of(), pagination, 0, DatabaseType.POSTGRES, null, null, null))
                .build())
        .build();
  }

  @SneakyThrows
  private static <T> T join(Future<T> future) {
    return future.toCompletionStage().toCompletableFuture().get(2, TimeUnit.MINUTES);
  }

  private static final String CREATE_ORDERS =
      """
      CREATE TABLE orders (
        id INT, customer TEXT, amount DOUBLE PRECISION, total NUMERIC(12, 2), quantity BIGINT,
        paid BOOLEAN, created_at TIMESTAMPTZ, day DATE, payload JSONB, tags TEXT[])
      """;

  private static final String INSERT_ORDERS =
      """
      INSERT INTO orders SELECT
        i,
        CASE WHEN i %% 7 = 0 THEN NULL ELSE 'customer ' || i || ' "é"' END,
        i / 3.0,
        (i * 1.25)::NUMERIC(12, 2),
        i::BIGINT * 1000003,
        i %% 2 = 0,
        TIMESTAMPTZ '2024-01-01 00:00:00+00' + i * INTERVAL '1 second',
        DATE '2024-01-01' + (i %% 365),
        CASE WHEN i %% 5 = 0 THEN NULL ELSE jsonb_build_object('n', i, 'even', i %% 2 = 0) END,
        CASE WHEN i %% 3 = 0 THEN NULL ELSE ARRAY['a' || i, NULL] END
      FROM generate_series(1, %d) AS i
      """
          .formatted(ROWS);
}
