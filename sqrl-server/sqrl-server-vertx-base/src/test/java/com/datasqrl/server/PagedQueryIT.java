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
package com.datasqrl.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.server.graphql.CustomScalars;
import com.datasqrl.server.graphql.GraphQLEngineBuilder;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.graphql.RootGraphQLModel.ArgumentLookupQueryCoords;
import com.datasqrl.server.graphql.RootGraphQLModel.QueryWithArguments;
import com.datasqrl.server.graphql.RootGraphQLModel.SqlQuery;
import com.datasqrl.server.graphql.RootGraphQLModel.StringSchema;
import com.datasqrl.server.jdbc.DatabaseType;
import com.datasqrl.server.jdbc.VertxJdbcClient;
import com.datasqrl.server.jdbc.VertxParamArgumentTypeMapper;
import graphql.ExecutionInput;
import graphql.GraphQL;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.pgclient.PgBuilder;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.PrepareOptions;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.Tuple;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Collector;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Proves that pagination metadata is computed lazily from the selection set: the aggregate query
 * only runs when totals or event times are selected, event times pick the MIN/MAX variant, and
 * {@code hasNextPage} alone is answered by fetching LIMIT+1 rows instead of any count query.
 */
@ExtendWith(VertxExtension.class)
@Testcontainers
class PagedQueryIT {

  private static final String BASE_SQL = "SELECT customerid, ts FROM customer ORDER BY customerid";
  private static final String COUNT_SQL =
      "SELECT COUNT(*) AS \"total_records\" FROM (" + BASE_SQL + ") x";
  private static final String COUNT_WITH_EVENT_TIMES_SQL =
      "SELECT COUNT(*) AS \"total_records\", MIN(\"ts\") AS \"first_event_time\","
          + " MAX(\"ts\") AS \"last_event_time\" FROM ("
          + BASE_SQL
          + ") x";

  @Container
  private static final PostgreSQLContainer postgresContainer =
      new PostgreSQLContainer(DockerImageName.parse("postgres:16"))
          .withDatabaseName("datasqrl")
          .withUsername("foo")
          .withPassword("secret");

  private SqlClient client;
  private RecordingSqlClient recordingClient;
  private GraphQL graphQL;

  @BeforeEach
  void init(Vertx vertx) {
    var options = new PgConnectOptions();
    options.setDatabase(postgresContainer.getDatabaseName());
    options.setHost(postgresContainer.getHost());
    options.setPort(postgresContainer.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT));
    options.setUser(postgresContainer.getUsername());
    options.setPassword(postgresContainer.getPassword());

    client = PgBuilder.client().with(new PoolOptions()).connectingTo(options).using(vertx).build();
    await(client.query("DROP TABLE IF EXISTS customer").execute());
    await(client.query("CREATE TABLE customer (customerid INT, ts TIMESTAMPTZ)").execute());
    await(
        client
            .query(
                """
                INSERT INTO customer VALUES
                  (1, '2024-01-01T00:00:00Z'),
                  (2, '2024-01-02T00:00:00Z'),
                  (3, '2024-01-03T00:00:00Z'),
                  (4, '2024-01-04T00:00:00Z'),
                  (5, '2024-01-05T00:00:00Z')
                """)
            .execute());

    recordingClient = new RecordingSqlClient(client);
    graphQL =
        getPagedModel()
            .accept(
                new GraphQLEngineBuilder.Builder()
                    .withExtendedScalarTypes(CustomScalars.getExtendedScalars())
                    .build(),
                new VertxServerContext(
                    new VertxJdbcClient(Map.of(DatabaseType.POSTGRES, recordingClient)),
                    null,
                    null,
                    new VertxParamArgumentTypeMapper()))
            .build();
    recordingClient.executed.clear();
  }

  @AfterEach
  void after() {
    client.close();
  }

  @Test
  void givenOnlyResultsSelected_whenQuery_thenNoAggregateQueryRuns() {
    var customers = execute("{ customers(limit: 2, offset: 0) { results { customerid } } }");

    assertThat(results(customers)).extracting("customerid").containsExactly(1, 2);
    assertThat(recordingClient.executed).hasSize(1);
    assertThat(recordingClient.executed.get(0).params().getInteger(0)).isEqualTo(2);
  }

  @Test
  void givenOnlyOffsetDerivedFieldsSelected_whenQuery_thenNoAggregateQueryRuns() {
    var customers =
        execute(
            "{ customers(limit: 2, offset: 2) { results { customerid }"
                + " pagination { pageSize currentPage hasPreviousPage prevOffset } } }");

    assertThat(recordingClient.executed).hasSize(1);
    var pagination = pagination(customers);
    assertThat(pagination)
        .containsEntry("pageSize", 2)
        .containsEntry("currentPage", 2)
        .containsEntry("hasPreviousPage", true)
        .containsEntry("prevOffset", 0);
  }

  @Test
  void givenOnlyHasNextPageSelected_whenQuery_thenLimitPlusOneReplacesCount() {
    var customers =
        execute(
            "{ customers(limit: 2, offset: 0) { results { customerid }"
                + " pagination { hasNextPage nextOffset } } }");

    assertThat(recordingClient.executed).hasSize(1);
    // the single data query fetched limit+1 rows and the extra row was trimmed
    assertThat(recordingClient.executed.get(0).params().getInteger(0)).isEqualTo(3);
    assertThat(results(customers)).extracting("customerid").containsExactly(1, 2);
    assertThat(pagination(customers))
        .containsEntry("hasNextPage", true)
        .containsEntry("nextOffset", 2);
  }

  @Test
  void givenHasNextPageSelectedOnLastPage_whenQuery_thenNoNextPage() {
    var customers =
        execute(
            "{ customers(limit: 2, offset: 4) { results { customerid }"
                + " pagination { hasNextPage nextOffset } } }");

    assertThat(recordingClient.executed).hasSize(1);
    assertThat(results(customers)).extracting("customerid").containsExactly(5);
    assertThat(pagination(customers)).containsEntry("hasNextPage", false);
  }

  @Test
  void givenTotalsSelected_whenQuery_thenPlainCountRunsWithoutExtraRow() {
    var customers =
        execute(
            "{ customers(limit: 2, offset: 0) { results { customerid }"
                + " pagination { totalRecords totalPages hasNextPage } } }");

    assertThat(recordingClient.executed).hasSize(2);
    assertThat(sqlOf(recordingClient.executed)).contains(COUNT_SQL);
    // hasNextPage is derived from the count, so the data query does not fetch an extra row
    var dataStatement =
        recordingClient.executed.stream().filter(s -> !s.sql().contains("COUNT")).findFirst();
    assertThat(dataStatement).isPresent();
    assertThat(dataStatement.get().params().getInteger(0)).isEqualTo(2);
    assertThat(pagination(customers))
        .containsEntry("totalRecords", 5L)
        .containsEntry("totalPages", 3)
        .containsEntry("hasNextPage", true);
  }

  @Test
  void givenEventTimesSelected_whenQuery_thenMinMaxVariantRuns() {
    var customers =
        execute(
            "{ customers(limit: 2, offset: 2) { results { customerid }"
                + " pagination { totalRecords firstEventTime lastEventTime } } }");

    assertThat(recordingClient.executed).hasSize(2);
    assertThat(sqlOf(recordingClient.executed)).contains(COUNT_WITH_EVENT_TIMES_SQL);
    var pagination = pagination(customers);
    assertThat(pagination).containsEntry("totalRecords", 5L);
    // MIN/MAX cover the whole result, not just the requested page
    assertThat(String.valueOf(pagination.get("firstEventTime"))).startsWith("2024-01-01");
    assertThat(String.valueOf(pagination.get("lastEventTime"))).startsWith("2024-01-05");
  }

  @Test
  void givenNoLimitArgument_whenQuery_thenPageSizeReportsRowCountNotSentinel() {
    var customers =
        execute(
            "{ customers: customersUnbounded { results { customerid }"
                + " pagination { pageSize totalRecords hasNextPage } } }");

    assertThat(results(customers)).hasSize(5);
    // an absent limit fetches every row; pageSize reflects the rows returned, not Integer.MAX_VALUE
    assertThat(pagination(customers))
        .containsEntry("pageSize", 5)
        .containsEntry("totalRecords", 5L)
        .containsEntry("hasNextPage", false);
  }

  @Test
  void givenEventTimesSelectedButNoRowtime_whenQuery_thenPlainCountRunsAndEventTimesAreNull() {
    var customers =
        execute(
            "{ customers: customersNoRowtime(limit: 2) { results { customerid }"
                + " pagination { totalRecords firstEventTime lastEventTime } } }");

    assertThat(recordingClient.executed).hasSize(2);
    // no rowtime => the MIN/MAX variant is absent; the plain count runs and event times stay null
    assertThat(sqlOf(recordingClient.executed)).contains(COUNT_SQL);
    assertThat(sqlOf(recordingClient.executed)).doesNotContain(COUNT_WITH_EVENT_TIMES_SQL);
    var pagination = pagination(customers);
    assertThat(pagination).containsEntry("totalRecords", 5L);
    assertThat(pagination.get("firstEventTime")).isNull();
    assertThat(pagination.get("lastEventTime")).isNull();
  }

  @SneakyThrows
  private Map<String, Object> execute(String query) {
    var result = graphQL.execute(ExecutionInput.newExecutionInput().query(query).build());
    assertThat(result.getErrors()).isEmpty();
    Map<String, Object> data = result.getData();
    return (Map<String, Object>) data.get("customers");
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> results(Map<String, Object> customers) {
    return (List<Map<String, Object>>) customers.get("results");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> pagination(Map<String, Object> customers) {
    return (Map<String, Object>) customers.get("pagination");
  }

  private static List<String> sqlOf(List<ExecutedStatement> statements) {
    return statements.stream().map(ExecutedStatement::sql).toList();
  }

  private RootGraphQLModel getPagedModel() {
    return RootGraphQLModel.builder()
        .schema(
            StringSchema.builder()
                .schema(
                    """
                scalar DateTime
                scalar Long
                type Query {
                  customers(limit: Int = 10, offset: Int = 0): CustomerPage!
                  customersUnbounded(limit: Int, offset: Int = 0): CustomerPage!
                  customersNoRowtime(limit: Int = 10, offset: Int = 0): CustomerPage!
                }
                type Customer {
                  customerid: Int
                }
                type CustomerPage {
                  results: [Customer!]
                  pagination: OffsetPageInfo
                }
                type OffsetPageInfo {
                  totalRecords: Long!
                  pageSize: Int!
                  currentPage: Int!
                  totalPages: Int!
                  hasNextPage: Boolean!
                  hasPreviousPage: Boolean!
                  nextOffset: Int
                  prevOffset: Int
                  firstEventTime: DateTime
                  lastEventTime: DateTime
                }
                """)
                .build())
        .query(
            ArgumentLookupQueryCoords.builder()
                .parentType("Query")
                .fieldName("customers")
                .exec(
                    QueryWithArguments.builder()
                        .query(
                            new SqlQuery(
                                BASE_SQL,
                                List.of(),
                                PaginationType.LIMIT_AND_OFFSET,
                                0,
                                DatabaseType.POSTGRES,
                                COUNT_SQL,
                                COUNT_WITH_EVENT_TIMES_SQL))
                        .build())
                .build())
        .query(
            ArgumentLookupQueryCoords.builder()
                .parentType("Query")
                .fieldName("customersUnbounded")
                .exec(
                    QueryWithArguments.builder()
                        .query(
                            new SqlQuery(
                                BASE_SQL,
                                List.of(),
                                PaginationType.LIMIT_AND_OFFSET,
                                0,
                                DatabaseType.POSTGRES,
                                COUNT_SQL,
                                COUNT_WITH_EVENT_TIMES_SQL))
                        .build())
                .build())
        .query(
            ArgumentLookupQueryCoords.builder()
                .parentType("Query")
                .fieldName("customersNoRowtime")
                .exec(
                    QueryWithArguments.builder()
                        .query(
                            new SqlQuery(
                                BASE_SQL,
                                List.of(),
                                PaginationType.LIMIT_AND_OFFSET,
                                0,
                                DatabaseType.POSTGRES,
                                COUNT_SQL,
                                null))
                        .build())
                .build())
        .build();
  }

  @SneakyThrows
  private static <T> T await(Future<T> future) {
    return future.toCompletionStage().toCompletableFuture().get();
  }

  private record ExecutedStatement(String sql, Tuple params) {}

  /** Delegating {@link SqlClient} that records every executed statement with its bound tuple. */
  private static final class RecordingSqlClient implements SqlClient {

    private final SqlClient delegate;
    final List<ExecutedStatement> executed = new CopyOnWriteArrayList<>();

    RecordingSqlClient(SqlClient delegate) {
      this.delegate = delegate;
    }

    @Override
    public Query<RowSet<Row>> query(String sql) {
      return delegate.query(sql);
    }

    @Override
    public PreparedQuery<RowSet<Row>> preparedQuery(String sql) {
      return recording(sql, delegate.preparedQuery(sql));
    }

    @Override
    public PreparedQuery<RowSet<Row>> preparedQuery(String sql, PrepareOptions options) {
      return recording(sql, delegate.preparedQuery(sql, options));
    }

    @Override
    public Future<Void> close() {
      return delegate.close();
    }

    private PreparedQuery<RowSet<Row>> recording(String sql, PreparedQuery<RowSet<Row>> delegate) {
      return new PreparedQuery<>() {
        @Override
        public Future<RowSet<Row>> execute(Tuple tuple) {
          executed.add(new ExecutedStatement(sql, tuple));
          return delegate.execute(tuple);
        }

        @Override
        public Future<RowSet<Row>> execute() {
          executed.add(new ExecutedStatement(sql, Tuple.tuple()));
          return delegate.execute();
        }

        @Override
        public Future<RowSet<Row>> executeBatch(List<Tuple> batch) {
          throw new UnsupportedOperationException();
        }

        @Override
        public <R> PreparedQuery<SqlResult<R>> collecting(Collector<Row, ?, R> collector) {
          throw new UnsupportedOperationException();
        }

        @Override
        public <U> PreparedQuery<RowSet<U>> mapping(Function<Row, U> mapper) {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
}
