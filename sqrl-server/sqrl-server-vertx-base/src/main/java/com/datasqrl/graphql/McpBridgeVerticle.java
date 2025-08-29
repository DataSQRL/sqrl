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
package com.datasqrl.graphql;

import com.datasqrl.graphql.auth.JwtFailureHandler;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.operation.ApiOperation;
import com.datasqrl.graphql.server.operation.McpMethodType;
import com.datasqrl.util.ProjectConstants;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import graphql.language.OperationDefinition.Operation;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.JWTAuthHandler;
import java.io.IOException;
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
  private static final String CT_JSON = "application/json";
  private static final String CT_SSE = "text/event-stream";

  private static final String RESOURCES_RESULT_KEY = "resources";
  private static final String RESOURCE_TEMPLATES_RESULT_KEY = "resourceTemplates";

  private final ConcurrentHashMap<String, SseConnection> sseConnections = new ConcurrentHashMap<>();

  private final Map<String, ApiOperation> tools;
  private final JsonObject toolsList;
  private final List<ApiOperation> resources;
  private final JsonObject resourceList;
  private final JsonObject resourceTemplatesList;

  public McpBridgeVerticle(
      Router router,
      ServerConfig config,
      String modelVersion,
      RootGraphqlModel model,
      Optional<JWTAuth> jwtAuth,
      GraphQLServerVerticle graphQLServerVerticle) {
    super(router, config, modelVersion, model, jwtAuth, graphQLServerVerticle);
    this.tools =
        model.getOperations().stream()
            .filter(op -> op.getMcpMethod() == McpMethodType.TOOL)
            .collect(Collectors.toMap(ApiOperation::getId, Function.identity()));
    this.toolsList = getToolsList(this.tools.values());
    this.resources =
        model.getOperations().stream()
            .filter(op -> op.getMcpMethod() == McpMethodType.RESOURCE)
            .toList();
    this.resourceList =
        getResourceList(
            RESOURCES_RESULT_KEY,
            resources.stream()
                .filter(op -> op.getFunction().getParameters().getProperties().isEmpty())
                .collect(Collectors.toList()));
    this.resourceTemplatesList =
        getResourceList(
            RESOURCE_TEMPLATES_RESULT_KEY,
            resources.stream()
                .filter(op -> !op.getFunction().getParameters().getProperties().isEmpty())
                .collect(Collectors.toList()));
  }

  @Override
  public void start(Promise<Void> startPromise) {
    String mcpRoutePrefix = config.getServletConfig().getMcpEndpoint(modelVersion);

    // MCP SSE endpoint - handles POST for messages
    var postRoute = router.post(mcpRoutePrefix);

    // Add JWT auth if configured
    jwtAuth.ifPresent(
        auth -> {
          log.info("Applying JWT authentication to MCP endpoint: {}", mcpRoutePrefix);
          postRoute.handler(JWTAuthHandler.create(auth));
          postRoute.failureHandler(new JwtFailureHandler());
        });

    postRoute.handler(
        ctx -> {
          JsonNode payload = null;
          try {
            payload = OBJECT_MAPPER.readTree(ctx.body().buffer().getBytes());
            processIncomingMessages(ctx, payload);
          } catch (IOException e) { // TODO: improve error handling
            throw new RuntimeException(e);
          }
        });

    // SSE endpoint for server-to-client streaming
    var sseRoute = router.get(mcpRoutePrefix + "/sse");

    // Add JWT auth to SSE endpoint if configured
    jwtAuth.ifPresent(
        auth -> {
          log.info("Applying JWT authentication to MCP SSE endpoint: {}/sse", mcpRoutePrefix);
          sseRoute.handler(JWTAuthHandler.create(auth));
          sseRoute.failureHandler(new JwtFailureHandler());
        });

    sseRoute.handler(
        ctx -> {
          if (!accepts(ctx, CT_SSE)) {
            ctx.response().setStatusCode(406).end();
            return;
          }
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
                log.info("SSE connection closed: {}", connectionId);
              });

          // Handle client disconnect
          response.exceptionHandler(
              throwable -> {
                vertx.cancelTimer(timerId);
                sseConnections.remove(connectionId);
                log.warn("SSE connection error: {} - {}", connectionId, throwable.getMessage());
              });
        });
    startPromise.complete();
  }

  private void sendSseMessage(SseConnection connection, String event, JsonObject data) {
    try {
      String message = String.format("event: %s\ndata: %s\n\n", event, data.encode());
      connection.response.write(message);
    } catch (Exception e) {
      log.error("Error sending SSE message: {}", e.getMessage());
      sseConnections.remove(connection.id);
    }
  }

  // Broadcast a message to all connected SSE clients
  public void broadcast(String event, JsonObject data) {
    sseConnections.values().forEach(connection -> sendSseMessage(connection, event, data));
  }

  public Future<JsonObject> handleRequest(RoutingContext ctx, JsonNode request) {
    String method = request.get("method").asText();
    JsonNode id = request.get("id");
    JsonNode params = request.get("params");

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
            case "resources/templates/list" -> Future.succeededFuture(resourceTemplatesList);
            case "resources/read" -> handleReadResource(ctx, params);
            case "ping" -> Future.succeededFuture(new JsonObject());
            default -> Future.failedFuture(new McpException(-32601, "Method not found"));
          };

      return resultFuture
          .map(
              result -> {
                return createResponse(id, result);
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
                  log.error("Tool call error response: {}", errorResponse.encode());
                }

                return Future.succeededFuture(errorResponse);
              });
    } catch (ValidationException e) {
      JsonObject json = new JsonObject();
      json.put(
          "content",
          List.of(
              new JsonObject()
                  .put("type", "text")
                  .put("text", "Validation error: " + e.getMessage())));
      json.put("isError", true);
      JsonObject response = createResponse(id, json);
      return Future.succeededFuture(response);
    } catch (Exception e) {
      JsonObject errorResponse =
          createErrorResponse(id, -32603, "Internal error: " + e.getMessage());
      log.error("Request error: {}", errorResponse.encode());
      return Future.succeededFuture(errorResponse);
    }
  }

  /**
   * Handle notifications (messages without id field). Notifications should not return responses.
   */
  private Future<JsonObject> handleNotification(String method, JsonNode params) {
    switch (method) {
      case "notifications/initialized":
        // Handle the initialized notification - just log it
        log.info("Client initialized notification received");
        break;
      case "notifications/cancelled":
        // Handle cancellation notifications if needed
        log.info("Request cancelled notification received");
        break;
      default:
        // Unknown notifications are ignored (as per JSON-RPC spec)
        log.warn("Unknown notification: {}", method);
        break;
    }

    // Return null to indicate no response should be sent for notifications
    return Future.succeededFuture(null);
  }

  private JsonObject handleInitialize(JsonNode params) {
    JsonObject capabilities =
        new JsonObject()
            .put("tools", new JsonObject())
            .put("resources", new JsonObject().put("subscribe", false).put("listChanged", false));

    JsonObject serverInfo =
        new JsonObject()
            .put("name", "datasqrl-mcp-server")
            .put("version", ProjectConstants.SQRL_VERSION);

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
  private Future<JsonObject> handleReadResource(RoutingContext ctx, JsonNode params) {
    String uri = params.get("uri").asText();
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

  private Future<JsonObject> handleCallTool(RoutingContext ctx, JsonNode params)
      throws ValidationException {
    String toolName = params.get("name").asText();
    JsonNode arguments = params.get("arguments");

    ApiOperation tool = tools.get(toolName);
    if (tool == null) {
      return Future.failedFuture(new McpException(-32602, "Tool not found: " + toolName));
    }
    Map<String, Object> variables = OBJECT_MAPPER.convertValue(arguments, new TypeReference<>() {});
    return bridgeRequestToGraphQL(ctx, tool, variables)
        .map(
            executionResult -> {
              JsonObject json = new JsonObject();
              if (!executionResult.getErrors().isEmpty()) {
                json.put(
                    "content",
                    executionResult.getErrors().stream()
                        .map(
                            err ->
                                new JsonObject()
                                    .put("type", "text")
                                    .put(
                                        "text",
                                        "Tool Error[" + err.getPath() + "]: " + err.getMessage()))
                        .toList());
                json.put("isError", true);
              } else {
                Object result = getExecutionData(executionResult, tool);
                try {
                  json.put(
                      "content",
                      List.of(
                          new JsonObject()
                              .put("type", "text")
                              .put("text", OBJECT_MAPPER.writeValueAsString(result))));
                  json.put("isError", false);
                } catch (JsonProcessingException e) {
                  throw new RuntimeException(e);
                }
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
          getSchemaMapper()
              .convertValue(
                  tool.getFunction().getParameters(), new TypeReference<Map<String, Object>>() {});
      String description = tool.getFunction().getDescription();
      if (description == null) {
        description =
            "Invokes %s %s"
                .formatted(
                    tool.getFunction().getName(),
                    tool.getApiQuery().operationType().name().toLowerCase());
      }
      boolean isReadOnly = tool.getApiQuery().operationType() != Operation.MUTATION;
      JsonObject toolInfo =
          new JsonObject()
              .put("name", tool.getName())
              .put("description", description)
              .put("inputSchema", inputSchema)
              .put("annotations", new JsonObject().put("readOnlyHint", isReadOnly));
      toolsArray.add(toolInfo);
    }
    return new JsonObject().put("tools", toolsArray);
  }

  private JsonObject getResourceList(
      String resultKey, Collection<ApiOperation> resourceOperations) {
    JsonArray resourcesArray = new JsonArray();
    for (ApiOperation resource : resourceOperations) {
      String description = resource.getFunction().getDescription();
      if (description == null) {
        description = "Returns %s resource".formatted(resource.getFunction().getName());
      }
      JsonObject resourceDef =
          new JsonObject()
              .put("uri", resource.getUriTemplate())
              .put("name", resource.getName())
              .put("description", description)
              .put("mimeType", "application/json");
      resourcesArray.add(resourceDef);
    }
    return new JsonObject().put(resultKey, resourcesArray);
  }

  private JsonObject createResponse(Object id, JsonObject result) {
    return new JsonObject().put("jsonrpc", JSONRPC_VERSION).put("id", id).put("result", result);
  }

  private JsonObject createErrorResponse(Object id, int code, String message) {
    JsonObject error = new JsonObject().put("code", code).put("message", message);
    return new JsonObject().put("jsonrpc", JSONRPC_VERSION).put("id", id).put("error", error);
  }

  // ---------------------------------------------------------------------------
  // MCP Streamable‑HTTP helpers
  // ---------------------------------------------------------------------------

  private void processIncomingMessages(RoutingContext ctx, JsonNode payload) {
    boolean containsRequest = containsRequestObjects(payload);

    // Pure notifications (no "method" requests) → 202 Accepted
    if (!containsRequest) {
      ctx.response().setStatusCode(202).end();
      return;
    }

    boolean clientAcceptsSse =
        accepts(ctx, CT_SSE) && !accepts(ctx, CT_JSON); // prefer JSON if both present

    if (clientAcceptsSse && requestNeedsStreaming(payload)) {
      streamResponse(ctx, payload);
    } else {
      singleJsonResponse(ctx, payload);
    }
  }

  private void singleJsonResponse(RoutingContext ctx, JsonNode payload) {
    if (!payload.isObject()) {
      ctx.fail(400, new IllegalArgumentException("Batch requests not supported yet"));
      return;
    }
    handleRequest(ctx, payload)
        .onSuccess(
            result -> {
              log.info("Result: {}", result);
              if (result != null) {
                ctx.response().putHeader(HttpHeaders.CONTENT_TYPE, CT_JSON).end(result.encode());
              } else {
                ctx.response().setStatusCode(204).end();
              }
            });
  }

  private void streamResponse(RoutingContext ctx, JsonNode payload) {
    if (!payload.isObject()) {
      ctx.fail(400, new IllegalArgumentException("Batch requests not supported in streaming mode"));
      return;
    }
    HttpServerResponse res = ctx.response();
    res.setChunked(true)
        .putHeader(HttpHeaders.CONTENT_TYPE, CT_SSE)
        .putHeader("Cache-Control", "no-cache");

    handleRequest(ctx, payload)
        .onSuccess(
            result -> {
              // send single chunk – for true multi‑chunk you’d wire this to reactive execution
              res.write("data: " + result.encode() + "\n\n").onComplete(ar -> res.end());
            })
        .onFailure(
            err -> {
              res.write(
                      "data: " + new JsonObject().put("error", err.getMessage()).encode() + "\n\n")
                  .onComplete(ar -> res.end());
            });
  }

  private static boolean containsRequestObjects(JsonNode node) {
    if (node.isArray()) {
      for (JsonNode n : node) {
        if (n.hasNonNull("method")) return true;
      }
      return false;
    }
    return node.hasNonNull("method");
  }

  /** Minimal heuristic: initialise & streamed tool calls need SSE */
  private static boolean requestNeedsStreaming(JsonNode payload) {
    if (!payload.isObject()) return false;
    String method = payload.path("method").asText("");
    return "initialize".equals(method)
        || ("tools/call".equals(method) && payload.path("params").path("stream").asBoolean(false));
  }

  private static boolean accepts(RoutingContext ctx, String mime) {
    List<String> accept = ctx.request().headers().getAll(HttpHeaders.ACCEPT);
    return accept.stream().anyMatch(h -> h.contains(mime));
  }

  private record SseConnection(String id, HttpServerResponse response) {}

  static class McpException extends RuntimeException {
    final int code;

    McpException(int code, String message) {
      super(message);
      this.code = code;
    }
  }
}
