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

import static com.datasqrl.server.stream.OrdersApi.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import graphql.GraphQLContext;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.Tuple;
import java.util.List;
import java.util.Locale;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class RowStreamResponseTest {

  private static final String SQL = "SELECT id FROM orders";
  private static final int ROWS_PAST_ONE_CHUNK = 10_000;

  private final HttpServerResponse response = mock(HttpServerResponse.class);
  private final Pool pool = mock(Pool.class);
  private final SqlConnection connection = mock(SqlConnection.class);
  private final Transaction transaction = mock(Transaction.class);
  private final FakeRowStream rows = new FakeRowStream();
  private StreamPlan plan;
  private RowStreamResponse streamResponse;
  private PendingStream statement;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void init() {
    var client = mock(SqlClient.class);
    when(client.preparedQuery(anyString())).thenReturn(mock(PreparedQuery.class));
    plan =
        OrdersApi.eligibility(OrdersApi.engine(client))
            .plan("{ orders { id } }", null)
            .orElseThrow();
    streamResponse =
        new RowStreamResponse(
            response,
            plan,
            new RowJsonEncoder(plan, GraphQLContext.getDefault(), Locale.getDefault()));
    statement = new PendingStream();
    statement.bind(SQL, List.of(7));

    var prepared = mock(PreparedStatement.class);
    when(pool.getConnection()).thenReturn(Future.succeededFuture(connection));
    when(connection.begin()).thenReturn(Future.succeededFuture(transaction));
    when(connection.prepare(SQL)).thenReturn(Future.succeededFuture(prepared));
    when(prepared.createStream(eq(50), any(Tuple.class))).thenReturn(rows);
    when(transaction.rollback()).thenReturn(Future.succeededFuture());
    when(connection.close()).thenReturn(Future.succeededFuture());
  }

  @Test
  void givenSmallResult_whenCursorEnds_thenSendsWholeResponseAndReleasesConnection() {
    streamResponse.start(pool, statement, 50);
    emit(3);
    rows.endHandler.handle(null);

    assertThat(ended().toString())
        .isEqualTo("{\"data\":{\"orders\":[{\"id\":0},{\"id\":1},{\"id\":2}]}}");
    verify(response, never()).setChunked(true);
    assertReleased();
  }

  @Test
  void givenWriteQueueFull_whenChunkWritten_thenCursorPausesUntilResponseDrains() {
    when(response.writeQueueFull()).thenReturn(true);
    streamResponse.start(pool, statement, 50);

    emit(ROWS_PAST_ONE_CHUNK);

    assertThat(rows.paused).isTrue();
    verify(response).setChunked(true);
    var drainHandler = handlerCaptor();
    verify(response, atLeastOnce()).drainHandler(drainHandler.capture());
    drainHandler.getValue().handle(null);
    assertThat(rows.paused).isFalse();
  }

  @Test
  void givenLargeResult_whenCursorEnds_thenChunksAndEndFormTheCompleteResponse() {
    streamResponse.start(pool, statement, 50);
    emit(ROWS_PAST_ONE_CHUNK);
    rows.endHandler.handle(null);

    var body = new JsonObject(written().appendBuffer(ended()));
    assertThat(body.getJsonObject("data").getJsonArray("orders")).hasSize(ROWS_PAST_ONE_CHUNK);
    assertThat(body.containsKey("errors")).isFalse();
    assertReleased();
  }

  @Test
  void givenCursorFailsBeforeFirstChunk_whenFailed_thenSendsTheNonStreamingErrorResponse() {
    var failure = new IllegalStateException("canceling statement due to statement timeout");
    streamResponse.start(pool, statement, 50);
    emit(3);

    rows.exceptionHandler.handle(failure);

    assertThat(ended()).isEqualTo(plan.errorResponse(failure));
    assertReleased();
  }

  @Test
  void givenCursorFailsAfterFirstChunk_whenFailed_thenTerminatesRowsAndAppendsTheError() {
    streamResponse.start(pool, statement, 50);
    emit(ROWS_PAST_ONE_CHUNK);

    rows.exceptionHandler.handle(new IllegalStateException("terminating connection"));

    var body = new JsonObject(written().appendBuffer(ended()));
    assertThat(body.getJsonObject("data").getJsonArray("orders")).hasSize(ROWS_PAST_ONE_CHUNK);
    assertThat(body.getJsonArray("errors").getJsonObject(0).getString("message"))
        .isEqualTo("Exception while fetching data (/orders) : terminating connection");
    assertReleased();
  }

  @Test
  void givenClientDisconnects_whenStreaming_thenCursorClosesAndNothingMoreIsWritten() {
    streamResponse.start(pool, statement, 50);
    emit(3);

    closeHandler().handle(null);
    emit(3);
    rows.endHandler.handle(null);

    verify(response, never()).end(any(Buffer.class));
    assertReleased();
  }

  @Test
  void givenClientGoneBeforeConnectionAcquired_whenConnectionArrives_thenItIsReleasedUnused() {
    var acquired = Promise.<SqlConnection>promise();
    when(pool.getConnection()).thenReturn(acquired.future());
    streamResponse.start(pool, statement, 50);

    closeHandler().handle(null);
    acquired.complete(connection);

    verify(connection, never()).begin();
    verify(connection).close();
    verify(response, never()).end(any(Buffer.class));
  }

  private void emit(int count) {
    OrdersApi.rows(List.of("id"), IntStream.range(0, count).mapToObj(i -> row(i)).toList())
        .forEach(row -> rows.handler.handle(row));
  }

  private Handler<Void> closeHandler() {
    var captor = handlerCaptor();
    verify(response).closeHandler(captor.capture());
    return captor.getValue();
  }

  @SuppressWarnings("unchecked")
  private static ArgumentCaptor<Handler<Void>> handlerCaptor() {
    return ArgumentCaptor.forClass(Handler.class);
  }

  private Buffer written() {
    var captor = ArgumentCaptor.forClass(Buffer.class);
    verify(response, atLeastOnce()).write(captor.capture());
    var body = Buffer.buffer();
    captor.getAllValues().forEach(body::appendBuffer);
    return body;
  }

  private Buffer ended() {
    var captor = ArgumentCaptor.forClass(Buffer.class);
    verify(response).end(captor.capture());
    return captor.getValue();
  }

  private void assertReleased() {
    assertThat(rows.closed).isTrue();
    verify(transaction).rollback();
    verify(connection).close();
  }

  private static final class FakeRowStream implements RowStream<Row> {

    Handler<Row> handler;
    Handler<Void> endHandler;
    Handler<Throwable> exceptionHandler;
    boolean paused;
    boolean closed;

    @Override
    public RowStream<Row> exceptionHandler(Handler<Throwable> handler) {
      exceptionHandler = handler;
      return this;
    }

    @Override
    public RowStream<Row> handler(Handler<Row> handler) {
      this.handler = handler;
      return this;
    }

    @Override
    public RowStream<Row> pause() {
      paused = true;
      return this;
    }

    @Override
    public RowStream<Row> resume() {
      paused = false;
      return this;
    }

    @Override
    public RowStream<Row> endHandler(Handler<Void> handler) {
      endHandler = handler;
      return this;
    }

    @Override
    public RowStream<Row> fetch(long amount) {
      return this;
    }

    @Override
    public Future<Void> close() {
      closed = true;
      return Future.succeededFuture();
    }
  }
}
