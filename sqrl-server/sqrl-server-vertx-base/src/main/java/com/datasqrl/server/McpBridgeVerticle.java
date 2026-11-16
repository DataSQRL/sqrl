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
package com.datasqrl.server;

import com.datasqrl.server.auth.OAuthFailureHandler;
import com.datasqrl.server.config.ServerConfig;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.operation.ApiOperation;
import com.datasqrl.server.operation.McpMethodType;
import com.datasqrl.util.JsonUtils;
import com.datasqrl.util.ProjectConstants;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import graphql.ExecutionResult;
import graphql.GraphQLError;
import graphql.language.OperationDefinition.Operation;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.oauth2.OAuth2Auth;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.handler.ChainAuthHandler;
import io.vertx.ext.web.handler.JWTAuthHandler;
import io.vertx.ext.web.handler.OAuth2AuthHandler;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class McpBridgeVerticle extends AbstractBridgeVerticle {

  private static final String JSONRPC_VERSION = "2.0";
  // Supported MCP protocol versions, ordered from oldest to latest
  static final List<String> PROTOCOL_VERSIONS =
      List.of("2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25");
  static final String LATEST_PROTOCOL_VERSION = PROTOCOL_VERSIONS.get(PROTOCOL_VERSIONS.size() - 1);
  private static final String CT_JSON = "application/json";
  private static final String CT_SSE = "text/event-stream";

  private static final String RESOURCES_RESULT_KEY = "resources";
  private static final String RESOURCE_TEMPLATES_RESULT_KEY = "resourceTemplates";

  static final int INVALID_PARAMS = -32602;
  static final int INTERNAL_ERROR = -32603;
  static final int RESOURCE_NOT_FOUND = -32002;

  private final ConcurrentHashMap<String, SseConnection> sseConnections = new ConcurrentHashMap<>();

  private final Map<String, ApiOperation> tools;
  private final JsonObject toolsList;
  private final Map<String, McpResource> staticResources = new LinkedHashMap<>();
  private final List<McpResource> resourceTemplates = new ArrayList<>();
  private final JsonObject resourceList;
  private final JsonObject resourceTemplatesList;

  public McpBridgeVerticle(
      Router router,
      ServerConfig config,
      String modelVersion,
      RootGraphQLModel model,
      List<AuthenticationProvider> authProviders,
      GraphQLServerVerticle graphQLServerVerticle) {
    super(router, config, modelVersion, model, authProviders, graphQLServerVerticle);
    this.tools =
        model.getOperations().stream()
            .filter(op -> op.getMcpMethod() == McpMethodType.TOOL)
            .collect(Collectors.toMap(ApiOperation::getId, Function.identity()));
    this.toolsList = getToolsList(this.tools.values());

    var uriPrefix =
        Objects.requireNonNullElse(
                config.getMcpResourceUriPrefix(), ServerConfig.DEFAULT_MCP_RESOURCE_URI_PREFIX)
            .replace("{version}", modelVersion);
    for (var operation : model.getOperations()) {
      if (operation.getMcpMethod() != McpMethodType.RESOURCE) {
        continue;
      }
      if (operation.getUriTemplate() == null) {
        log.warn("Skipping MCP resource {} - missing uriTemplate", operation.getName());
        continue;
      }
      var uriTemplate = toResourceUri(uriPrefix, operation.getUriTemplate());
      var resource = new McpResource(operation, uriTemplate, new UriTemplateMatcher(uriTemplate));
      if (hasParameters(operation)) {
        resourceTemplates.add(resource);
      } else {
        staticResources.putIfAbsent(uriTemplate, resource);
      }
    }
    this.resourceList = getResourceList(RESOURCES_RESULT_KEY, "uri", staticResources.values());
    this.resourceTemplatesList =
        getResourceList(RESOURCE_TEMPLATES_RESULT_KEY, "uriTemplate", resourceTemplates);
  }

  /** Puts the prefix in front of the operation's path template, without its leading slash. */
  static String toResourceUri(String uriPrefix, String pathTemplate) {
    return uriPrefix + (pathTemplate.startsWith("/") ? pathTemplate.substring(1) : pathTemplate);
  }

  private static boolean hasParameters(ApiOperation operation) {
    var parameters = operation.getFunction().getParameters();
    return parameters != null
        && parameters.getProperties() != null
        && !parameters.getProperties().isEmpty();
  }

  @Override
  public void start(Promise<Void> startPromise) {
    var mcpRoutePrefix = config.getServletConfig().getMcpEndpoint(modelVersion);
    log.info("Starting McpBridgeVerticle with endpoint prefix: {}", mcpRoutePrefix);
    log.info("Available tools: {}", tools.keySet());
    log.info(
        "Available resources: {}, resource templates: {}",
        staticResources.keySet(),
        resourceTemplates.stream().map(McpResource::uriTemplate).toList());

    // Debug: Log ALL incoming requests to help identify what the MCP client is doing
    router
        .route("/*")
        .handler(
            ctx -> {
              String path = ctx.request().path();
              if (path.contains("/mcp") || path.contains("/v1")) {
                log.info(
                    "Request received: {} {} - Headers: {}",
                    ctx.request().method(),
                    path,
                    ctx.request().headers().names());
              }
              ctx.next();
            });

    // Add CORS support for MCP endpoint
    router
        .route(mcpRoutePrefix + "*")
        .handler(
            ctx -> {
              log.info("MCP route matched: {} {}", ctx.request().method(), ctx.request().path());
              ctx.response()
                  .putHeader("Access-Control-Allow-Origin", "*")
                  .putHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                  .putHeader("Access-Control-Allow-Headers", "Content-Type, Accept, Authorization");
              ctx.next();
            });

    // Handle preflight OPTIONS requests
    router
        .options(mcpRoutePrefix)
        .handler(
            ctx -> {
              log.debug("Handling OPTIONS request for MCP endpoint");
              ctx.response().setStatusCode(204).end();
            });

    // Add a catch-all GET handler for MCP endpoint to help with debugging
    router
        .get(mcpRoutePrefix)
        .handler(
            ctx -> {
              log.info(
                  "Received GET request to MCP endpoint - this might be a client capability check");
              ctx.response()
                  .setStatusCode(405)
                  .putHeader("Content-Type", "application/json")
                  .putHeader("Allow", "POST, OPTIONS")
                  .end(
                      "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32601,\"message\":\"Method not allowed. Use POST for MCP requests.\"}}");
            });

    // MCP endpoint - handles POST for messages
    var postRoute = router.post(mcpRoutePrefix);
    var authFailureHandler =
        new OAuthFailureHandler(config.getProtectedResourceMetadata().orElse(null));

    // Add JWT/OAuth auth if configured
    if (!authProviders.isEmpty()) {
      log.info(
          "Applying {} authentication provider(s) to MCP endpoint: {}",
          authProviders.size(),
          mcpRoutePrefix);
      postRoute.handler(createChainAuthHandler());
      postRoute.failureHandler(authFailureHandler);
    }

    postRoute.handler(
        ctx -> {
          log.debug("Received POST request to MCP endpoint: {}", mcpRoutePrefix);
          try {
            var payload = JsonUtils.MAPPER.readTree(ctx.body().buffer().getBytes());
            log.debug("Parsed payload: {}", payload);
            processIncomingMessages(ctx, payload);
          } catch (IOException e) { // TODO: improve error handling
            log.error("Failed to parse MCP request payload", e);
            ctx.response()
                .setStatusCode(400)
                .putHeader("Content-Type", "application/json")
                .end(
                    "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32700,\"message\":\"Parse error\"}}");
          }
        });

    // SSE endpoint for server-to-client streaming
    var sseRoute = router.get(mcpRoutePrefix + "/sse");

    // Add JWT/OAuth auth to SSE endpoint if configured
    if (!authProviders.isEmpty()) {
      log.info(
          "Applying {} authentication provider(s) to MCP SSE endpoint: {}/sse",
          authProviders.size(),
          mcpRoutePrefix);
      sseRoute.handler(createChainAuthHandler());
      sseRoute.failureHandler(authFailureHandler);
    }

    sseRoute.handler(
        ctx -> {
          log.info("Received SSE connection request to: {}/sse", mcpRoutePrefix);
          if (!accepts(ctx, CT_SSE)) {
            log.warn("SSE request rejected - client doesn't accept text/event-stream");
            ctx.response().setStatusCode(406).end();
            return;
          }
          var response = ctx.response();

          // Set up SSE headers
          response
              .putHeader("Content-Type", "text/event-stream")
              .putHeader("Cache-Control", "no-cache")
              .putHeader("Connection", "keep-alive")
              .putHeader("Access-Control-Allow-Origin", "*")
              .setChunked(true);

          var connectionId = UUID.randomUUID().toString();
          var connection = new SseConnection(connectionId, response);
          sseConnections.put(connectionId, connection);
          log.info("New SSE connection established: {}", connectionId);

          // Send initial connection event
          sendSseMessage(
              connection, "connected", new JsonObject().put("connectionId", connectionId));

          // Send heartbeat every 30 seconds
          var timerId =
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
    var method = request.get("method").asText();
    var id = request.get("id");
    var params = request.get("params");

    log.info(
        "Handling MCP request - method: {}, id: {}", method, id != null ? id.asText() : "null");
    log.debug("Request params: {}", params);

    // Handle notifications (messages without id) - these should not return responses
    if (id == null) {
      log.debug("Processing notification for method: {}", method);
      return handleNotification(method, params);
    }

    try {
      Future<JsonObject> resultFuture =
          switch (method) {
            case "initialize" -> {
              log.info("Handling initialize request");
              yield Future.succeededFuture(handleInitialize(params));
            }
            case "tools/list" -> {
              log.info("Handling tools/list request - returning {} tools", tools.size());
              yield Future.succeededFuture(toolsList);
            }
            case "tools/call" -> {
              log.info("Handling tools/call request");
              yield handleCallTool(ctx, params);
            }
            case "resources/list" -> {
              log.info(
                  "Handling resources/list request - returning {} resources",
                  staticResources.size());
              yield Future.succeededFuture(resourceList);
            }
            case "resources/templates/list" -> {
              log.info("Handling resources/templates/list request");
              yield Future.succeededFuture(resourceTemplatesList);
            }
            case "resources/read" -> {
              log.info("Handling resources/read request");
              yield handleReadResource(ctx, params);
            }
            case "ping" -> {
              log.debug("Handling ping request");
              yield Future.succeededFuture(new JsonObject());
            }
            default -> {
              log.warn("Unknown MCP method: {}", method);
              yield Future.failedFuture(new McpException(-32601, "Method not found"));
            }
          };

      return resultFuture
          .map(
              result -> {
                log.debug("Request {} completed successfully", method);
                return createResponse(id, result);
              })
          .recover(
              error -> {
                log.error(
                    "Error handling MCP request method '{}': {}",
                    method,
                    error.getMessage(),
                    error);
                var errorResponse =
                    error instanceof McpException e
                        ? createErrorResponse(id, e.code, e.getMessage(), e.data)
                        : createErrorResponse(
                            id, INTERNAL_ERROR, "Internal error: " + error.getMessage(), null);

                // Enhanced debugging for tool call errors
                if ("tools/call".equals(method)) {
                  log.error("Tool call error response: {}", errorResponse.encode());
                }

                return Future.succeededFuture(errorResponse);
              });
    } catch (ValidationException e) {
      var json = new JsonObject();
      json.put(
          "content",
          List.of(
              new JsonObject()
                  .put("type", "text")
                  .put("text", "Validation error: " + e.getMessage())));
      json.put("isError", true);
      var response = createResponse(id, json);
      return Future.succeededFuture(response);
    } catch (Exception e) {
      var errorResponse =
          createErrorResponse(id, INTERNAL_ERROR, "Internal error: " + e.getMessage(), null);
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
    var protocolVersion = negotiateProtocolVersion(params);
    log.info("Initializing MCP server with protocol version: {}", protocolVersion);
    var capabilities =
        new JsonObject()
            .put("tools", new JsonObject())
            .put("resources", new JsonObject().put("subscribe", false).put("listChanged", false));

    var serverInfo =
        new JsonObject()
            .put("name", "datasqrl-mcp-server")
            .put("version", ProjectConstants.SQRL_VERSION);

    var response =
        new JsonObject()
            .put("protocolVersion", protocolVersion)
            .put("capabilities", capabilities)
            .put("serverInfo", serverInfo);

    log.info("Initialize response: {}", response.encodePrettily());
    return response;
  }

  /**
   * Echoes the protocol version requested by the client if it is supported, otherwise falls back to
   * the latest supported version, as mandated by the MCP lifecycle specification.
   */
  static String negotiateProtocolVersion(JsonNode params) {
    var requested = params == null ? null : params.path("protocolVersion").asText(null);
    if (requested != null && PROTOCOL_VERSIONS.contains(requested)) {
      return requested;
    }
    return LATEST_PROTOCOL_VERSION;
  }

  private Future<JsonObject> handleReadResource(RoutingContext ctx, JsonNode params) {
    var uri = params == null ? null : params.path("uri").asText(null);
    if (uri == null || uri.isBlank()) {
      return Future.failedFuture(new McpException(INVALID_PARAMS, "URI parameter required"));
    }

    final Optional<ResourceMatch> match;
    try {
      match = findResource(uri);
    } catch (IllegalArgumentException e) {
      return Future.failedFuture(
          new McpException(INVALID_PARAMS, "Invalid resource URI: " + e.getMessage()));
    }
    if (match.isEmpty()) {
      return Future.failedFuture(
          new McpException(
              RESOURCE_NOT_FOUND, "Resource not found", new JsonObject().put("uri", uri)));
    }

    var operation = match.get().resource().operation();
    var variables = new HashMap<String, Object>();
    final Future<ExecutionResult> execution;
    try {
      extractUriParameters(
          operation,
          match.get().parameters().queryParameters(),
          match.get().parameters().pathParameters(),
          variables);
      execution = bridgeRequestToGraphQL(ctx, operation, variables);
    } catch (ValidationException e) {
      return Future.failedFuture(new McpException(INVALID_PARAMS, e.getMessage()));
    }

    return execution.compose(
        executionResult -> {
          if (!executionResult.getErrors().isEmpty()) {
            var messages =
                executionResult.getErrors().stream()
                    .map(GraphQLError::getMessage)
                    .collect(Collectors.joining("; "));
            return Future.failedFuture(
                new McpException(INTERNAL_ERROR, "Resource query failed: " + messages));
          }
          try {
            var text =
                JsonUtils.MAPPER.writeValueAsString(getExecutionData(executionResult, operation));
            var contents =
                new JsonArray()
                    .add(
                        new JsonObject()
                            .put("uri", uri)
                            .put("mimeType", CT_JSON)
                            .put("text", text));
            return Future.succeededFuture(new JsonObject().put("contents", contents));
          } catch (JsonProcessingException e) {
            return Future.failedFuture(e);
          }
        });
  }

  private Optional<ResourceMatch> findResource(String uri) {
    var staticResource = staticResources.get(uri);
    if (staticResource != null) {
      return Optional.of(
          new ResourceMatch(staticResource, new UriTemplateMatcher.Match(Map.of(), Map.of())));
    }
    for (var template : resourceTemplates) {
      var parameters = template.matcher().match(uri);
      if (parameters.isPresent()) {
        return Optional.of(new ResourceMatch(template, parameters.get()));
      }
    }
    return Optional.empty();
  }

  private Future<JsonObject> handleCallTool(RoutingContext ctx, JsonNode params) {
    var toolName = params.get("name").asText();
    var arguments = params.get("arguments");

    var tool = tools.get(toolName);
    if (tool == null) {
      return Future.failedFuture(new McpException(-32602, "Tool not found: " + toolName));
    }
    var variables =
        JsonUtils.MAPPER.<Map<String, Object>>convertValue(arguments, new TypeReference<>() {});
    return bridgeRequestToGraphQL(ctx, tool, variables)
        .map(
            executionResult -> {
              var json = new JsonObject();
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
                var result = getExecutionData(executionResult, tool);
                try {
                  json.put(
                      "content",
                      List.of(
                          new JsonObject()
                              .put("type", "text")
                              .put("text", JsonUtils.MAPPER.writeValueAsString(result))));
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
              var errorContent =
                  new JsonArray()
                      .add(
                          new JsonObject()
                              .put("type", "text")
                              .put("text", "Tool error: " + error.getMessage()));

              var errorResult = new JsonObject().put("content", errorContent).put("isError", true);

              return Future.succeededFuture(errorResult);
            });
  }

  private JsonObject getToolsList(Collection<ApiOperation> toolOperations) {
    var toolsArray = new JsonArray();

    for (var tool : toolOperations) {
      var inputSchema =
          getSchemaMapper()
              .convertValue(
                  tool.getFunction().getParameters(), new TypeReference<Map<String, Object>>() {});
      var description = tool.getFunction().getDescription();
      if (description == null) {
        description =
            "Invokes %s %s"
                .formatted(
                    tool.getFunction().getName(),
                    tool.getApiQuery().operationType().name().toLowerCase());
      }
      var isReadOnly = tool.getApiQuery().operationType() != Operation.MUTATION;
      var toolInfo =
          new JsonObject()
              .put("name", tool.getName())
              .put("description", description)
              .put("inputSchema", inputSchema)
              .put("annotations", new JsonObject().put("readOnlyHint", isReadOnly));
      toolsArray.add(toolInfo);
    }
    return new JsonObject().put("tools", toolsArray);
  }

  private static JsonObject getResourceList(
      String resultKey, String uriKey, Collection<McpResource> resources) {
    var resourcesArray = new JsonArray();
    for (var resource : resources) {
      var function = resource.operation().getFunction();
      var description = function.getDescription();
      if (description == null) {
        description = "Returns %s resource".formatted(function.getName());
      }
      var resourceDef =
          new JsonObject()
              .put(uriKey, resource.uriTemplate())
              .put("name", resource.operation().getName())
              .put("description", description)
              .put("mimeType", CT_JSON);
      resourcesArray.add(resourceDef);
    }
    return new JsonObject().put(resultKey, resourcesArray);
  }

  private JsonObject createResponse(Object id, JsonObject result) {
    return new JsonObject().put("jsonrpc", JSONRPC_VERSION).put("id", id).put("result", result);
  }

  private JsonObject createErrorResponse(
      Object id, int code, String message, @Nullable JsonObject data) {
    var error = new JsonObject().put("code", code).put("message", message);
    if (data != null) {
      error.put("data", data);
    }
    return new JsonObject().put("jsonrpc", JSONRPC_VERSION).put("id", id).put("error", error);
  }

  // ---------------------------------------------------------------------------
  // MCP Streamable‑HTTP helpers
  // ---------------------------------------------------------------------------

  private void processIncomingMessages(RoutingContext ctx, JsonNode payload) {
    var containsRequest = containsRequestObjects(payload);

    // Pure notifications (no "method" requests) → 202 Accepted
    if (!containsRequest) {
      ctx.response().setStatusCode(202).end();
      return;
    }

    var clientAcceptsSse =
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
              log.debug("Request completed successfully");
              if (result != null) {
                ctx.response().putHeader(HttpHeaders.CONTENT_TYPE, CT_JSON).end(result.encode());
              } else {
                ctx.response().setStatusCode(204).end();
              }
            })
        .onFailure(
            error -> {
              log.error("Request failed: {}", error.getMessage(), error);
              ctx.response()
                  .setStatusCode(500)
                  .putHeader("Content-Type", "application/json")
                  .end(
                      "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32603,\"message\":\"Internal error\"}}");
            });
  }

  private void streamResponse(RoutingContext ctx, JsonNode payload) {
    if (!payload.isObject()) {
      ctx.fail(400, new IllegalArgumentException("Batch requests not supported in streaming mode"));
      return;
    }
    var res = ctx.response();
    res.setChunked(true)
        .putHeader(HttpHeaders.CONTENT_TYPE, CT_SSE)
        .putHeader("Cache-Control", "no-cache");

    handleRequest(ctx, payload)
        .onSuccess(
            result -> {
              // send single chunk – for true multi‑chunk you'd wire this to reactive execution
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
      for (var n : node) {
        if (n.hasNonNull("method")) return true;
      }
      return false;
    }
    return node.hasNonNull("method");
  }

  /** Minimal heuristic: initialise & streamed tool calls need SSE */
  private static boolean requestNeedsStreaming(JsonNode payload) {
    if (!payload.isObject()) return false;
    var method = payload.path("method").asText("");
    return "initialize".equals(method)
        || ("tools/call".equals(method) && payload.path("params").path("stream").asBoolean(false));
  }

  private static boolean accepts(RoutingContext ctx, String mime) {
    var accept = ctx.request().headers().getAll(HttpHeaders.ACCEPT);
    return accept.stream().anyMatch(h -> h.contains(mime));
  }

  private AuthenticationHandler createChainAuthHandler() {
    if (authProviders.size() == 1) {
      return createAuthHandler(authProviders.get(0));
    }

    var chain = ChainAuthHandler.any();
    for (var provider : authProviders) {
      chain.add(createAuthHandler(provider));
    }
    return chain;
  }

  private AuthenticationHandler createAuthHandler(AuthenticationProvider auth) {
    if (auth instanceof JWTAuth jwtAuth) {
      return JWTAuthHandler.create(jwtAuth);
    } else if (auth instanceof OAuth2Auth oauth2Auth) {
      return OAuth2AuthHandler.create(vertx, oauth2Auth);
    } else {
      throw new IllegalArgumentException("Unsupported auth provider type: " + auth.getClass());
    }
  }

  private record SseConnection(String id, HttpServerResponse response) {}

  private record McpResource(
      ApiOperation operation, String uriTemplate, UriTemplateMatcher matcher) {}

  private record ResourceMatch(McpResource resource, UriTemplateMatcher.Match parameters) {}

  static class McpException extends RuntimeException {
    final int code;
    @Nullable final JsonObject data;

    McpException(int code, String message) {
      this(code, message, null);
    }

    McpException(int code, String message, @Nullable JsonObject data) {
      super(message);
      this.code = code;
      this.data = data;
    }
  }
}
