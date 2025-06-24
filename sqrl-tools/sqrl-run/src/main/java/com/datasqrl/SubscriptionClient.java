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
package com.datasqrl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.WebSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;

// Simplified example WebSocket client code using Vert.x
public class SubscriptionClient {
  private final String name;
  private final String query;
  private final List<String> messages = new ArrayList<>();
  private WebSocket webSocket;
  private final Vertx vertx = Vertx.vertx();

  private final ObjectMapper objectMapper = new ObjectMapper();
  private CompletableFuture<Void> connectedFuture = new CompletableFuture<>();

  public SubscriptionClient(String name, String query) {
    this.name = name;
    this.query = query;
  }

  public CompletableFuture<Void> start() {
    var connect = vertx.createWebSocketClient().connect(8888, "localhost", "/graphql");

    connect
        .onSuccess(
            ws -> {
              this.webSocket = ws;
              System.out.println("WebSocket opened for subscription: " + name);

              // Set a message handler for incoming messages
              ws.handler(this::handleTextMessage);

              // Send initial connection message
              sendConnectionInit()
                  .onComplete(success -> connectedFuture.complete(null))
                  .onFailure(error -> connectedFuture.completeExceptionally(error));
            })
        .onFailure(
            throwable -> {
              throwable.printStackTrace();
              System.err.println("Failed to open WebSocket for subscription: " + name);
              connectedFuture.completeExceptionally(throwable);
            });

    return connectedFuture;
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
