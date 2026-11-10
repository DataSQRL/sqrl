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
package com.datasqrl.server.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.server.util.LogCapture;
import graphql.execution.ResultPath;
import io.vertx.core.Future;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

class VertxJdbcClientTest {

  private static final JdbcStatement STATEMENT =
      new JdbcStatement(ResultPath.parse("/orders"), "SELECT * FROM orders");
  private static final Tuple PARAMS = Tuple.tuple();

  @SuppressWarnings("unchecked")
  private final RowSet<Row> rows = mock(RowSet.class);

  private final RecordingListener listener = new RecordingListener();

  @Test
  void givenNoListeners_whenExecute_thenReturnsTheStatementsOwnFuture() {
    var result = Future.succeededFuture(rows);
    var client = new VertxJdbcClient(Map.of());

    assertThat(client.execute(STATEMENT, query(result), PARAMS)).isSameAs(result);
  }

  @Test
  void givenStatementSucceeds_whenExecute_thenListenerSeesTheRowsBeforeTheCallerDoes() {
    var client = new VertxJdbcClient(Map.of(), List.of(listener));

    var seenByListenerFirst =
        client
            .execute(STATEMENT, query(Future.succeededFuture(rows)), PARAMS)
            .map(result -> List.copyOf(listener.observed));

    assertThat(await(seenByListenerFirst)).containsExactly(new Observed(STATEMENT, rows, null));
  }

  @Test
  void givenStatementFails_whenExecute_thenListenerSeesTheFailureAndTheFutureFails() {
    var failure = new IllegalStateException("relation \"orders\" does not exist");
    var client = new VertxJdbcClient(Map.of(), List.of(listener));

    var result = client.execute(STATEMENT, query(Future.failedFuture(failure)), PARAMS);

    assertThatThrownBy(() -> await(result)).hasRootCause(failure);
    assertThat(listener.observed).containsExactly(new Observed(STATEMENT, null, failure));
  }

  @Test
  void givenListenerThrows_whenExecute_thenStatementSucceedsAndLaterListenersStillRun() {
    var client = new VertxJdbcClient(Map.of(), List.of(new ThrowingListener(), listener));

    try (var log = LogCapture.of(VertxJdbcClient.class)) {
      var result = client.execute(STATEMENT, query(Future.succeededFuture(rows)), PARAMS);

      assertThat(await(result)).isSameAs(rows);
      assertThat(listener.observed).containsExactly(new Observed(STATEMENT, rows, null));
      assertThat(log.messages(Level.WARN))
          .containsExactly("Statement listener ThrowingListener failed at /orders");
    }
  }

  @SuppressWarnings("unchecked")
  private static PreparedQuery<RowSet<Row>> query(Future<RowSet<Row>> result) {
    PreparedQuery<RowSet<Row>> query = mock(PreparedQuery.class);
    when(query.execute(PARAMS)).thenReturn(result);
    return query;
  }

  @SneakyThrows
  private static <T> T await(Future<T> future) {
    return future.toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
  }

  private record Observed(JdbcStatement statement, RowSet<Row> rows, Throwable failure) {}

  private static final class RecordingListener implements JdbcStatementListener {

    private final List<Observed> observed = new CopyOnWriteArrayList<>();

    @Override
    public void onRows(JdbcStatement statement, RowSet<Row> rows, Duration elapsed) {
      observed.add(new Observed(statement, rows, null));
    }

    @Override
    public void onFailure(JdbcStatement statement, Throwable failure, Duration elapsed) {
      observed.add(new Observed(statement, null, failure));
    }
  }

  private static final class ThrowingListener implements JdbcStatementListener {

    @Override
    public void onRows(JdbcStatement statement, RowSet<Row> rows, Duration elapsed) {
      throw new IllegalStateException("listener bug");
    }
  }
}
