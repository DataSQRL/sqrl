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

import io.vertx.core.Future;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Pumps the rows of a server-side cursor into the HTTP response. The cursor is paused while the
 * response's write queue is full, and the connection is released on completion, on failure and when
 * the client goes away.
 */
@Slf4j
@RequiredArgsConstructor
public final class RowStreamResponse {

  static final int CHUNK_BYTES = 64 * 1024;

  private static final Exception CANCELLED = new IllegalStateException("Response already closed");

  private final HttpServerResponse response;
  private final StreamPlan plan;
  private final RowJsonEncoder encoder;

  private SqlConnection connection;
  private Transaction transaction;
  private RowStream<Row> rows;
  private boolean started;
  private boolean done;

  public void start(Pool pool, PendingStream statement, int fetchSize) {
    response.putHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    response.closeHandler(ignored -> abandon());
    response.exceptionHandler(ignored -> abandon());
    pool.getConnection()
        .compose(this::begin)
        .compose(tx -> prepare(tx, statement.getSql()))
        .onSuccess(prepared -> open(prepared.createStream(fetchSize, statement.getParams())))
        .onFailure(this::fail);
  }

  private Future<Transaction> begin(SqlConnection acquired) {
    connection = acquired;
    return done ? Future.failedFuture(CANCELLED) : acquired.begin();
  }

  private Future<PreparedStatement> prepare(Transaction tx, String sql) {
    transaction = tx;
    return done ? Future.failedFuture(CANCELLED) : connection.prepare(sql);
  }

  private void open(RowStream<Row> stream) {
    rows = stream;
    if (done) {
      release();
      return;
    }
    stream.exceptionHandler(this::fail);
    stream.endHandler(ignored -> complete());
    stream.handler(this::onRow);
  }

  private void onRow(Row row) {
    if (done) {
      return;
    }
    try {
      encoder.writeRow(row);
    } catch (RuntimeException e) {
      fail(e);
      return;
    }
    if (encoder.bufferedBytes() >= CHUNK_BYTES) {
      flush();
    }
  }

  private void flush() {
    if (!started) {
      started = true;
      response.setChunked(true);
    }
    response.write(encoder.drain());
    if (response.writeQueueFull()) {
      rows.pause();
      response.drainHandler(ignored -> resume());
    }
  }

  private void resume() {
    if (!done) {
      rows.resume();
    }
  }

  private void complete() {
    if (done) {
      return;
    }
    done = true;
    encoder.finish();
    response.end(encoder.drain());
    release();
  }

  private void fail(Throwable cause) {
    if (done) {
      release();
      return;
    }
    done = true;
    log.warn("Streaming the rows of '{}' failed", plan.responseKey(), cause);
    if (!response.closed()) {
      if (started) {
        encoder.fail(cause);
        response.end(encoder.drain());
      } else {
        response.end(plan.errorResponse(cause));
      }
    }
    release();
  }

  private void abandon() {
    if (!done) {
      done = true;
      log.info("Client went away while streaming the rows of '{}'", plan.responseKey());
      release();
    }
  }

  private void release() {
    var stream = rows;
    var tx = transaction;
    var conn = connection;
    rows = null;
    transaction = null;
    connection = null;
    if (conn == null) {
      return;
    }
    var closed = stream == null ? Future.<Void>succeededFuture() : stream.close();
    closed
        .eventually(() -> tx == null ? Future.succeededFuture() : tx.rollback())
        .eventually(conn::close);
  }
}
