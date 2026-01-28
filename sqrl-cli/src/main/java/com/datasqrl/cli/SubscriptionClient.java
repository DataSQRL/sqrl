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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class SubscriptionClient implements AutoCloseable {
  private static final int MAX_RETRIES = 3;
  private static final long INITIAL_DELAY_MS = 100;

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final HttpClient httpClient = HttpClient.newHttpClient();
  private final CompletableFuture<Void> connectedFuture = new CompletableFuture<>();
  @Getter private final List<String> messages = new ArrayList<>();

  private final String version;
  @Getter private final String name;
  private final String query;
  private final Map<String, String> headers;

  private WebSocket webSocket;

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
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        connectedFuture.completeExceptionally(e);
        return;
      }
    }

    connectWebSocket(attempt);
  }

  private void connectWebSocket(int attempt) {
    var uri = URI.create("ws://localhost:8888/%s/graphql".formatted(version));
    var builder = httpClient.newWebSocketBuilder();
    builder.subprotocols("graphql-transport-ws");
    headers.forEach(builder::header);

    builder
        .buildAsync(uri, new WebSocketListener())
        .whenComplete(
            (ws, throwable) -> {
              if (throwable != null) {
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
              } else {
                this.webSocket = ws;
                log.info("WebSocket opened for subscription: {}", name);
                sendConnectionInit();
              }
            });
  }

  private void sendConnectionInit() {
    sendMessage(Map.of("type", "connection_init"));
  }

  private void sendSubscribe() {
    Map<String, Object> payload = Map.of("query", query);
    Map<String, Object> message =
        Map.of("id", System.nanoTime(), "type", "subscribe", "payload", payload);
    sendMessage(message);
  }

  @SneakyThrows
  private void sendMessage(Map<String, Object> message) {
    String json = objectMapper.writeValueAsString(message);
    System.out.println("Sending: " + json);
    webSocket.sendText(json, true);
  }

  private void handleTextMessage(String data) {
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
      sendSubscribe();
      connectedFuture.complete(null);
    } else if ("complete".equals(type)) {
      // Subscription complete
    } else if ("error".equals(type)) {
      System.err.println("Error message received: " + data);
      throw new RuntimeException("Error data: " + data);
    } else {
      throw new RuntimeException("Unknown type " + type);
    }
  }

  @Override
  public void close() throws Exception {
    if (webSocket != null) {
      sendMessage(Map.of("id", System.nanoTime(), "type", "complete"));
      webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "closing").join();
    }
  }

  private class WebSocketListener implements WebSocket.Listener {
    private final StringBuilder textBuffer = new StringBuilder();

    @Override
    public void onOpen(WebSocket webSocket) {
      webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
      textBuffer.append(data);
      if (last) {
        handleTextMessage(textBuffer.toString());
        textBuffer.setLength(0);
      }
      webSocket.request(1);
      return null;
    }

    @Override
    public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
      webSocket.request(1);
      return null;
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
      log.info("WebSocket closed: {} - {}", statusCode, reason);
      return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
      log.error("WebSocket error for subscription: {}", name, error);
    }
  }
}
