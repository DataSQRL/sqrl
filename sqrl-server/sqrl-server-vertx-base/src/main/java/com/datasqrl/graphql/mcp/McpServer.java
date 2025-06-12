package com.datasqrl.graphql.mcp;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class McpServer extends AbstractVerticle {
  private final ConcurrentHashMap<String, SseConnection> sseConnections = new ConcurrentHashMap<>();
  private McpToolHandler handler;

  public McpServer() {}

  @Override
  public void start(Promise<Void> startPromise) {
    // Initialize protocolHandler here where vertx is available
    this.handler = new McpToolHandler(vertx);

    Router router = Router.router(vertx);

    // Configure body handler for POST requests
    router.route().handler(BodyHandler.create());

    // MCP SSE endpoint - handles POST for messages
    router
        .post("/mcp/sse")
        .handler(
            ctx -> {
              JsonObject body = ctx.body().asJsonObject();

              // Handle JSON-RPC request asynchronously
              handler
                  .handleRequestAsync(body)
                  .onComplete(
                      ar -> {
                        if (ar.succeeded()) {
                          JsonObject result = ar.result();
                          if (result != null) {
                            ctx.response()
                                .putHeader("Content-Type", "application/json")
                                .end(result.encode());
                          } else {
                            // For notifications that return null
                            ctx.response().setStatusCode(204).end();
                          }
                        } else {
                          ctx.response()
                              .setStatusCode(500)
                              .end(new JsonObject().put("error", ar.cause().getMessage()).encode());
                        }
                      });
            });

    // SSE endpoint for server-to-client streaming
    router
        .get("/mcp/sse")
        .handler(
            ctx -> {
              HttpServerResponse response = ctx.response();

              // Set up SSE headers
              response
                  .putHeader("Content-Type", "text/event-stream")
                  .putHeader("Cache-Control", "no-cache")
                  .putHeader("Connection", "keep-alive")
                  .putHeader("Access-Control-Allow-Origin", "*")
                  .setChunked(true);

              String connectionId = UUID.randomUUID().toString();
              SseConnection connection = new SseConnection(connectionId, response);
              sseConnections.put(connectionId, connection);

              // Send initial connection event
              sendSseMessage(
                  connection, "connected", new JsonObject().put("connectionId", connectionId));

              // Send heartbeat every 30 seconds
              long timerId =
                  vertx.setPeriodic(
                      30000,
                      id -> {
                        if (sseConnections.containsKey(connectionId)) {
                          sendSseMessage(
                              connection,
                              "heartbeat",
                              new JsonObject().put("timestamp", System.currentTimeMillis()));
                        }
                      });

              // Handle connection close
              response.closeHandler(
                  v -> {
                    vertx.cancelTimer(timerId);
                    sseConnections.remove(connectionId);
                    System.err.println("SSE connection closed: " + connectionId);
                  });

              // Handle client disconnect
              response.exceptionHandler(
                  throwable -> {
                    vertx.cancelTimer(timerId);
                    sseConnections.remove(connectionId);
                    System.err.println(
                        "SSE connection error: " + connectionId + " - " + throwable.getMessage());
                  });
            });

    // HTTP server setup
    HttpServerOptions options = new HttpServerOptions().setPort(8080).setHost("localhost");

    vertx
        .createHttpServer(options)
        .requestHandler(router)
        .listen()
        .onComplete(
            result -> {
              if (result.succeeded()) {
                System.err.println(
                    "HTTP/SSE MCP Server started on port " + result.result().actualPort());
                startPromise.complete();
              } else {
                startPromise.fail(result.cause());
              }
            });
  }

  private void sendSseMessage(SseConnection connection, String event, JsonObject data) {
    try {
      String message = String.format("event: %s\ndata: %s\n\n", event, data.encode());
      connection.response.write(message);
    } catch (Exception e) {
      System.err.println("Error sending SSE message: " + e.getMessage());
      sseConnections.remove(connection.id);
    }
  }

  // Broadcast a message to all connected SSE clients
  public void broadcast(String event, JsonObject data) {
    sseConnections.values().forEach(connection -> sendSseMessage(connection, event, data));
  }

  private static class SseConnection {
    final String id;
    final HttpServerResponse response;

    SseConnection(String id, HttpServerResponse response) {
      this.id = id;
      this.response = response;
    }
  }
}
