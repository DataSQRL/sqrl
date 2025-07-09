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
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

// Simplified example WebSocket client code using Vert.x
@RequiredArgsConstructor
@Slf4j
public class SubscriptionClient {
  private static final int MAX_RETRIES = 3;
  private static final long INITIAL_DELAY_MS = 100;

  private final String name;
  private final String query;
  private final Map<String, String> headers;
  private final List<String> messages = new ArrayList<>();
  private WebSocket webSocket;
  private final Vertx vertx = Vertx.vertx();

  private final ObjectMapper objectMapper = new ObjectMapper();
  private CompletableFuture<Void> connectedFuture = new CompletableFuture<>();

  public CompletableFuture<Void> start() {
    attemptConnection(0);
    return connectedFuture;
  }

  private void attemptConnection(int attempt) {
    if (attempt >= MAX_RETRIES) {
      log.error("Failed to connect after {} attempts for subscription: {}", MAX_RETRIES, name);
      connectedFuture.completeExceptionally(
          new RuntimeException("Failed to connect after " + MAX_RETRIES + " attempts"));
      return;
    }

    long delay = INITIAL_DELAY_MS * (long) Math.pow(2, attempt);

    if (attempt > 0) {
      log.info(
          "Attempting to reconnect (attempt {}/{}) for subscription: {} after {}ms delay",
          attempt + 1,
          MAX_RETRIES,
          name,
          delay);
      vertx.setTimer(delay, id -> connectWebSocket(attempt));
    } else {
      connectWebSocket(attempt);
    }
  }

  private void connectWebSocket(int attempt) {
    /* 1. Collect handshake headers */
    var headerMap = MultiMap.caseInsensitiveMultiMap();
    if (headers != null) {
      headers.forEach((k, v) -> headerMap.add(k, v));
    }

    /* 2. Describe the connection */
    var opts =
        new WebSocketConnectOptions()
            .setHost("localhost")
            .setPort(8888)
            .setURI("/graphql")
            .addSubProtocol("graphql-transport-ws") // or "graphql-ws"
            .setHeaders(headerMap);

    /* 3. Open the socket with the new WebSocketClient API */
    var wsClient = vertx.createWebSocketClient();
    wsClient
        .connect(opts)
        .onSuccess(
            ws -> {
              this.webSocket = ws;
              log.info("WebSocket opened for subscription: {}", name);

              // Set a message handler for incoming messages
              ws.handler(this::handleTextMessage);

              // Send initial connection message
              sendConnectionInit()
                  .onComplete(success -> connectedFuture.complete(null))
                  .onFailure(error -> connectedFuture.completeExceptionally(error));
            })
        .onFailure(
            throwable -> {
              if (attempt < MAX_RETRIES - 1) {
                log.warn(
                    "Failed to open WebSocket for subscription: {} (attempt {}/{}), retrying...",
                    name,
                    attempt + 1,
                    MAX_RETRIES,
                    throwable);
                attemptConnection(attempt + 1);
              } else {
                log.error(
                    "Failed to open WebSocket for subscription: {} after {} attempts",
                    name,
                    MAX_RETRIES,
                    throwable);
                connectedFuture.completeExceptionally(throwable);
              }
            });
  }

  private Future<Void> sendConnectionInit() {
    return sendMessage(Map.of("type", "connection_init"));
  }

  private Future<Void> sendSubscribe() {
    Map<String, Object> payload =
        Map.of(
            //              "operationName", "breakMe",
            "query", query);
    Map<String, Object> message =
        Map.of("id", System.nanoTime(), "type", "subscribe", "payload", payload);
    return sendMessage(message);
  }

  @SneakyThrows
  private Future<Void> sendMessage(Map<String, Object> message) {
    String json = objectMapper.writeValueAsString(message);
    System.out.println("Sending: " + json);
    return webSocket.writeTextMessage(json);
  }

  private void handleTextMessage(Buffer buffer) {
    var data = buffer.toString();
    // Handle the incoming messages
    System.out.println("Data: " + data);
    Map<String, Object> message;
    try {
      message = objectMapper.readValue(data, Map.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Unable to serialize message", e);
    }

    if (message.containsKey("payload")) {
      try {
        messages.add(objectMapper.writeValueAsString(message.get("payload")));
      } catch (JsonProcessingException e) {
        throw new RuntimeException("Unable to serialize message", e);
      }
      return;
    }

    var type = (String) message.get("type");

    if ("connection_ack".equals(type)) {
      // Connection acknowledged, send the subscribe message
      sendSubscribe();
      connectedFuture.complete(null);
    } else if ("complete".equals(type)) {
      // Subscription complete
    } else if ("error".equals(type)) {
      // Handle error
      System.err.println("Error message received: " + data);
      throw new RuntimeException("Error data: " + data);
    } else {
      throw new RuntimeException("Unknown type " + type);
    }
  }

  public void stop() {
    // Send 'complete' message to close the subscription properly
    Map<String, Object> message = Map.of("id", System.nanoTime(), "type", "complete");
    waitCompletion(sendMessage(message));

    // Close WebSocket
    if (webSocket != null) {
      waitCompletion(webSocket.close());
    }
    waitCompletion(vertx.close());
  }

  @SneakyThrows
  private <E> E waitCompletion(Future<E> future) {
    return future.toCompletionStage().toCompletableFuture().get();
  }

  public List<String> getMessages() {
    return messages;
  }

  public String getName() {
    return name;
  }
}
