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
package com.datasqrl.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketClient;
import io.vertx.core.http.WebSocketConnectOptions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

// Simplified example WebSocket client code using Vert.x
@RequiredArgsConstructor
@Slf4j
public class SubscriptionClient implements AutoCloseable {

  private static final int MAX_RETRIES = 5;
  private static final long INITIAL_DELAY_MS = 200;
  private static final long CONNECT_TIMEOUT_MS = 2_000;
  private static final long HANDSHAKE_TIMEOUT_MS = 5_000;
  private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(5);
  private static final long CLOSE_PHASE_TIMEOUT_MS = 6_000;

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Vertx vertx = Vertx.vertx();
  // All mutable connection state is accessed only from this context.
  private final Context context = vertx.getOrCreateContext();
  private final WebSocketClient wsClient = vertx.createWebSocketClient();
  private final CompletableFuture<Void> connectedFuture = new CompletableFuture<>();
  private final AtomicBoolean closeStarted = new AtomicBoolean();
  @Getter private final List<String> messages = new ArrayList<>();

  private final String version;
  @Getter private final String name;
  private final String query;
  private final Map<String, String> headers;

  private WebSocket webSocket;
  private long connectionId;
  private long retryTimerId = -1;
  private long startupTimerId = -1;
  private boolean started;
  private boolean closed;
  private String subscriptionId;
  private Future<Void> socketCloseFuture;

  public CompletableFuture<Void> start() {
    context.runOnContext(
        ignored -> {
          if (started) {
            return;
          }
          if (closed) {
            connectedFuture.completeExceptionally(
                new IllegalStateException("Subscription client is already closed"));
            return;
          }
          started = true;
          attemptConnection(0);
        });
    return connectedFuture;
  }

  private void attemptConnection(int attempt) {
    if (closed || connectedFuture.isDone()) {
      return;
    }
    if (attempt >= MAX_RETRIES) {
      log.error("Failed to connect after {} attempts for subscription: {}", MAX_RETRIES, name);
      connectedFuture.completeExceptionally(
          new RuntimeException("Failed to connect after " + MAX_RETRIES + " attempts"));
      return;
    }

    if (attempt > 0) {
      long delay = INITIAL_DELAY_MS * (1L << (attempt - 1));
      log.info(
          "Attempting to reconnect (attempt {}/{}) for subscription: {} after {}ms delay",
          attempt + 1,
          MAX_RETRIES,
          name,
          delay);

      retryTimerId =
          vertx.setTimer(
              delay,
              id -> {
                if (retryTimerId != id || closed || connectedFuture.isDone()) {
                  return;
                }
                retryTimerId = -1;
                connectWebSocket(attempt);
              });
    } else {
      connectWebSocket(attempt);
    }
  }

  private void connectWebSocket(int attempt) {
    if (closed || connectedFuture.isDone()) {
      return;
    }
    long currentConnectionId = ++connectionId;
    /* 1. Collect handshake headers */
    var headerMap = MultiMap.caseInsensitiveMultiMap();
    if (headers != null) {
      headers.forEach(headerMap::add);
    }

    /* 2. Describe the connection */
    var opts =
        new WebSocketConnectOptions()
            .setHost("localhost")
            .setPort(8888)
            .setURI("/%s/graphql".formatted(version))
            .addSubProtocol("graphql-transport-ws") // or "graphql-ws"
            .setConnectTimeout(CONNECT_TIMEOUT_MS)
            .setTimeout(HANDSHAKE_TIMEOUT_MS)
            .setHeaders(headerMap);

    wsClient
        .connect(opts)
        .onSuccess(
            ws -> {
              if (closed || currentConnectionId != connectionId || connectedFuture.isDone()) {
                ws.close();
                return;
              }
              this.webSocket = ws;
              log.info("WebSocket opened for subscription: {}", name);

              ws.exceptionHandler(error -> connectionFailed(currentConnectionId, attempt, error));
              ws.closeHandler(
                  ignored ->
                      connectionFailed(
                          currentConnectionId,
                          attempt,
                          new IllegalStateException(
                              "WebSocket closed before GraphQL connection acknowledgement")));
              ws.handler(buffer -> handleTextMessage(currentConnectionId, attempt, buffer));

              scheduleStartupTimeout(currentConnectionId, attempt, "GraphQL connection_ack");
              sendConnectionInit()
                  .onFailure(error -> connectionFailed(currentConnectionId, attempt, error));
            })
        .onFailure(throwable -> connectionFailed(currentConnectionId, attempt, throwable));
  }

  private void scheduleStartupTimeout(
      long currentConnectionId, int attempt, String pendingOperation) {
    startupTimerId =
        vertx.setTimer(
            HANDSHAKE_TIMEOUT_MS,
            id -> {
              if (startupTimerId == id
                  && currentConnectionId == connectionId
                  && !connectedFuture.isDone()) {
                connectionFailed(
                    currentConnectionId,
                    attempt,
                    new IllegalStateException("Timed out waiting for " + pendingOperation));
              }
            });
  }

  private void connectionFailed(long failedConnectionId, int attempt, Throwable error) {
    if (closed || connectedFuture.isDone() || failedConnectionId != connectionId) {
      return;
    }
    cancelStartupTimeout();
    subscriptionId = null;
    // Invalidate this generation before closing its socket: close handlers can run synchronously.
    connectionId++;
    if (webSocket != null) {
      var socket = webSocket;
      webSocket = null;
      socket.close();
    }

    if (attempt < MAX_RETRIES - 1) {
      log.warn(
          "Subscription WebSocket startup failed for {} (attempt {}/{}); retrying",
          name,
          attempt + 1,
          MAX_RETRIES,
          error);
      attemptConnection(attempt + 1);
    } else {
      log.error(
          "Subscription WebSocket startup failed for {} after {} attempts",
          name,
          MAX_RETRIES,
          error);
      connectedFuture.completeExceptionally(error);
    }
  }

  private void cancelStartupTimeout() {
    if (startupTimerId != -1) {
      vertx.cancelTimer(startupTimerId);
      startupTimerId = -1;
    }
  }

  private Future<Void> sendConnectionInit() {
    return sendMessage(Map.of("type", "connection_init"));
  }

  private Future<Void> sendSubscribe() {
    subscriptionId = Long.toUnsignedString(System.nanoTime());

    return sendMessage(
        Map.of("id", subscriptionId, "type", "subscribe", "payload", Map.of("query", query)));
  }

  private Future<Void> sendMessage(Map<String, Object> message) {
    return sendMessage(webSocket, message);
  }

  private Future<Void> sendMessage(WebSocket socket, Map<String, Object> message) {
    try {
      String json = objectMapper.writeValueAsString(message);
      if (socket == null || socket.isClosed()) {
        return Future.failedFuture("WebSocket is not open");
      }
      return socket.writeTextMessage(json);
    } catch (JsonProcessingException e) {
      return Future.failedFuture(e);
    }
  }

  private void handleTextMessage(long currentConnectionId, int attempt, Buffer buffer) {
    var data = buffer.toString();
    Map<String, Object> message;
    try {
      message = objectMapper.readValue(data, Map.class);
    } catch (JsonProcessingException e) {
      connectionFailed(currentConnectionId, attempt, e);
      return;
    }

    if (message.containsKey("payload")) {
      try {
        messages.add(objectMapper.writeValueAsString(message.get("payload")));
      } catch (JsonProcessingException e) {
        connectionFailed(currentConnectionId, attempt, e);
      }
      return;
    }

    var type = (String) message.get("type");

    if ("connection_ack".equals(type)) {
      if (closed
          || currentConnectionId != connectionId
          || connectedFuture.isDone()
          || subscriptionId != null) {
        return;
      }
      cancelStartupTimeout();
      scheduleStartupTimeout(currentConnectionId, attempt, "GraphQL subscribe write");
      sendSubscribe()
          .onSuccess(
              ignored -> {
                cancelStartupTimeout();
                connectedFuture.complete(null);
              })
          .onFailure(error -> connectionFailed(currentConnectionId, attempt, error));
    } else if ("ping".equals(type)) {
      sendMessage(Map.of("type", "pong"))
          .onFailure(error -> connectionFailed(currentConnectionId, attempt, error));
    } else if ("pong".equals(type)) {
      // Keep-alive acknowledgement.
    } else if ("complete".equals(type)) {
      // Subscription complete
    } else if ("error".equals(type)) {
      connectionFailed(currentConnectionId, attempt, new RuntimeException("Error data: " + data));
    } else {
      connectionFailed(
          currentConnectionId,
          attempt,
          new IllegalStateException("Unknown WebSocket message type " + type));
    }
  }

  @Override
  public void close() {
    if (!closeStarted.compareAndSet(false, true)) {
      return;
    }

    var socketClosePromise = Promise.<Void>promise();
    try {
      context.runOnContext(ignored -> closeSocketOnContext().onComplete(socketClosePromise));
    } catch (Throwable error) {
      socketClosePromise.fail(error);
    }

    awaitClose(socketClosePromise.future(), "subscription WebSocket");
    awaitClose(wsClient.close(), "WebSocket client");
    awaitClose(vertx.close(), "subscription Vert.x instance");
  }

  private Future<Void> closeSocketOnContext() {
    if (socketCloseFuture != null) {
      return socketCloseFuture;
    }
    closed = true;
    if (retryTimerId != -1) {
      vertx.cancelTimer(retryTimerId);
      retryTimerId = -1;
    }
    cancelStartupTimeout();

    socketCloseFuture =
        shutdownSocket()
            .recover(
                error -> {
                  log.debug("Unable to close subscription WebSocket {}", name, error);
                  return Future.succeededFuture();
                });
    return socketCloseFuture;
  }

  private Future<Void> shutdownSocket() {
    if (webSocket == null || webSocket.isClosed()) {
      return Future.succeededFuture();
    }

    var socket = webSocket;
    webSocket = null;
    // Vert.x force-closes the connection after this deadline if the peer does not finish the
    // WebSocket shutdown handshake. Closing the connection also cancels its GraphQL operations.
    return socket.shutdown(SHUTDOWN_TIMEOUT);
  }

  private void awaitClose(Future<?> future, String phase) {
    try {
      future
          .toCompletionStage()
          .toCompletableFuture()
          .get(CLOSE_PHASE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      log.warn(
          "Timed out after {}ms while closing {} for subscription {}",
          CLOSE_PHASE_TIMEOUT_MS,
          phase,
          name);
    } catch (ExecutionException e) {
      log.warn("Failed while closing {} for subscription {}", phase, name, e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn("Interrupted while closing {} for subscription {}", phase, name, e);
    }
  }
}
