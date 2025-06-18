package com.datasqrl.graphql;

import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.operation.ApiOperation;
import com.fasterxml.jackson.core.type.TypeReference;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class McpBridgeVerticle extends AbstractBridgeVerticle {

  private static final String JSONRPC_VERSION = "2.0";
  private static final String PROTOCOL_VERSION = "2024-11-05";

  private final ConcurrentHashMap<String, SseConnection> sseConnections = new ConcurrentHashMap<>();
  private final Map<String, ApiOperation> tools;
  private final JsonObject toolsList;
  private final List<ApiOperation> resources;
  private final JsonObject resourceList;

  public McpBridgeVerticle(
      Router router,
      ServerConfig config,
      RootGraphqlModel model,
      Optional<JWTAuth> jwtAuth,
      GraphQLServerVerticle graphQLServerVerticle) {
    super(router, config, model, jwtAuth, graphQLServerVerticle);
    this.tools =
        model.getOperations().stream()
            .filter(ApiOperation::isTool)
            .collect(Collectors.toMap(ApiOperation::getId, Function.identity()));
    this.toolsList = getToolsList(this.tools.values());
    this.resources = model.getOperations().stream().filter(ApiOperation::isResource).toList();
    this.resourceList = getResourceList(resources);
  }

  @Override
  public void start(Promise<Void> startPromise) {
    String mcpRoutePrefix = config.getServletConfig().getMcpEndpoint();

    // MCP SSE endpoint - handles POST for messages
    router
        .post(mcpRoutePrefix + "/sse")
        .handler(
            ctx -> {
              JsonObject body = ctx.body().asJsonObject();

              // Handle JSON-RPC request asynchronously
              handleRequest(ctx, body)
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
        .get(mcpRoutePrefix + "/sse")
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
    startPromise.complete();
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

  public Future<JsonObject> handleRequest(RoutingContext ctx, JsonObject request) {
    String method = request.getString("method");
    Object id = request.getValue("id");
    JsonObject params = request.getJsonObject("params", new JsonObject());

    // Enhanced debugging for tool calls
    if ("tools/call".equals(method)) {
      System.err.println("[DEBUG] Tool call request: " + request.encode());
    }

    // Handle notifications (messages without id) - these should not return responses
    if (id == null) {
      return handleNotification(method, params);
    }

    try {
      Future<JsonObject> resultFuture =
          switch (method) {
            case "initialize" -> Future.succeededFuture(handleInitialize(params));
            case "tools/list" -> Future.succeededFuture(toolsList);
            case "tools/call" -> handleCallTool(ctx, params);
            case "resources/list" -> Future.succeededFuture(resourceList);
            case "resources/read" -> handleReadResource(ctx, params);
            case "ping" -> Future.succeededFuture(new JsonObject());
            default -> Future.failedFuture(new McpException(-32601, "Method not found"));
          };

      return resultFuture
          .map(
              result -> {
                JsonObject response = createResponse(id, result);
                // Enhanced debugging for tool call responses
                if ("tools/call".equals(method)) {
                  System.err.println("[DEBUG] Tool call response: " + response.encode());
                }
                return response;
              })
          .recover(
              error -> {
                JsonObject errorResponse;
                if (error instanceof McpException e) {
                  errorResponse = createErrorResponse(id, e.code, e.getMessage());
                } else {
                  errorResponse =
                      createErrorResponse(id, -32603, "Internal error: " + error.getMessage());
                }

                // Enhanced debugging for tool call errors
                if ("tools/call".equals(method)) {
                  System.err.println("[DEBUG] Tool call error response: " + errorResponse.encode());
                }

                return Future.succeededFuture(errorResponse);
              });
    } catch (Exception e) {
      JsonObject errorResponse =
          createErrorResponse(id, -32603, "Internal error: " + e.getMessage());

      if ("tools/call".equals(method)) {
        System.err.println("[DEBUG] Tool call exception response: " + errorResponse.encode());
      }

      return Future.succeededFuture(errorResponse);
    }
  }

  /**
   * Handle notifications (messages without id field). Notifications should not return responses.
   */
  private Future<JsonObject> handleNotification(String method, JsonObject params) {
    switch (method) {
      case "notifications/initialized":
        // Handle the initialized notification - just log it
        System.err.println("Client initialized notification received");
        break;
      case "notifications/cancelled":
        // Handle cancellation notifications if needed
        System.err.println("Request cancelled notification received");
        break;
      default:
        // Unknown notifications are ignored (as per JSON-RPC spec)
        System.err.println("Unknown notification: " + method);
        break;
    }

    // Return null to indicate no response should be sent for notifications
    return Future.succeededFuture(null);
  }

  private JsonObject handleInitialize(JsonObject params) {
    JsonObject capabilities =
        new JsonObject()
            .put("tools", new JsonObject())
            .put("resources", new JsonObject().put("subscribe", false).put("listChanged", false));

    JsonObject serverInfo =
        new JsonObject().put("name", "datasqrl-mcp-server").put("version", "0.7.0");

    return new JsonObject()
        .put("protocolVersion", PROTOCOL_VERSION)
        .put("capabilities", capabilities)
        .put("serverInfo", serverInfo);
  }

  /**
   * Dummy implementation for now
   *
   * @param params
   * @return
   */
  private Future<JsonObject> handleReadResource(RoutingContext ctx, JsonObject params) {
    String uri = params.getString("uri");
    if (uri == null) {
      return Future.failedFuture(new McpException(-32602, "URI parameter required"));
    }
    if (uri.isBlank())
      return Future.failedFuture(new McpException(-32602, "Resource not found: " + uri));
    JsonArray contents =
        new JsonArray()
            .add(
                new JsonObject()
                    .put("uri", uri)
                    .put("mimeType", "application/json")
                    .put("text", new JsonObject().encodePrettily()));
    return Future.succeededFuture(new JsonObject().put("contents", contents));
  }

  private Future<JsonObject> handleCallTool(RoutingContext ctx, JsonObject params)
      throws Exception {
    String toolName = params.getString("name");
    JsonObject arguments = params.getJsonObject("arguments", new JsonObject());

    ApiOperation tool = tools.get(toolName);
    if (tool == null) {
      return Future.failedFuture(new McpException(-32602, "Tool not found: " + toolName));
    }

    return bridgeRequestToGraphQL(ctx, tool, arguments.getMap())
        .map(
            executionResult -> {
              JsonObject json;
              if (!executionResult.getErrors().isEmpty()) {
                json = new JsonObject();
                json.put(
                    "content",
                    executionResult.getErrors().stream()
                        .map(
                            err ->
                                new JsonObject()
                                    .put("type", "text")
                                    .put("text", "Tool Error:" + err.getPath())
                                    .put("extensions", err.getExtensions()))
                        .toList());
                json.put("isError", true);
              } else {
                json = new JsonObject((Map) executionResult.getData());
              }
              return json;
            })
        .recover(
            error -> {
              // Return tool error within the result (not as MCP protocol error)
              JsonArray errorContent =
                  new JsonArray()
                      .add(
                          new JsonObject()
                              .put("type", "text")
                              .put("text", "Tool error: " + error.getMessage()));

              JsonObject errorResult =
                  new JsonObject().put("content", errorContent).put("isError", true);

              return Future.succeededFuture(errorResult);
            });
  }

  private JsonObject getToolsList(Collection<ApiOperation> toolOperations) {
    JsonArray toolsArray = new JsonArray();

    for (ApiOperation tool : toolOperations) {
      Map<String, Object> inputSchema =
          objectMapper.convertValue(
              tool.getFunction().getParameters(), new TypeReference<Map<String, Object>>() {});
      JsonObject toolInfo =
          new JsonObject()
              .put("name", tool.getName())
              .put("description", tool.getFunction().getDescription())
              .put("inputSchema", inputSchema);
      toolsArray.add(toolInfo);
    }
    return new JsonObject().put("tools", toolsArray);
  }

  private JsonObject getResourceList(Collection<ApiOperation> resourceOperations) {
    JsonArray resourcesArray = new JsonArray();
    for (ApiOperation resource : resourceOperations) {
      JsonObject resourceDef =
          new JsonObject()
              .put("uri", resource.getUriTemplate())
              .put("name", resource.getName())
              .put("description", resource.getFunction().getDescription())
              .put("mimeType", "application/json");
      resourcesArray.add(resourceDef);
    }
    return new JsonObject().put("resources", resourcesArray);
  }

  private JsonObject createResponse(Object id, JsonObject result) {
    return new JsonObject().put("jsonrpc", JSONRPC_VERSION).put("id", id).put("result", result);
  }

  private JsonObject createErrorResponse(Object id, int code, String message) {
    JsonObject error = new JsonObject().put("code", code).put("message", message);
    return new JsonObject().put("jsonrpc", JSONRPC_VERSION).put("id", id).put("error", error);
  }

  static class McpException extends RuntimeException {
    final int code;

    McpException(int code, String message) {
      super(message);
      this.code = code;
    }
  }
}
