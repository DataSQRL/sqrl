package com.datasqrl.graphql.mcp;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.HashMap;
import java.util.Map;

public class McpToolHandler {
  private static final String JSONRPC_VERSION = "2.0";
  private static final String PROTOCOL_VERSION = "2024-11-05";

  private final Map<String, AsyncTool> tools = new HashMap<>();
  private final Vertx vertx;

  public McpToolHandler(Vertx vertx) {
    this.vertx = vertx;

    // Register async tools with proper input schemas
    registerTool(
        new AsyncTool("echo", "Echoes back the input", createEchoSchema(), this::echoTool));
  }

  private JsonObject createEchoSchema() {
    return new JsonObject()
        .put("type", "object")
        .put(
            "properties",
            new JsonObject()
                .put(
                    "message",
                    new JsonObject()
                        .put("type", "string")
                        .put("description", "The message to echo back")))
        .put("required", new JsonArray().add("message"));
  }

  public void registerTool(AsyncTool tool) {
    tools.put(tool.name, tool);
  }

  public Future<JsonObject> handleRequestAsync(JsonObject request) {
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
            case "tools/list" -> Future.succeededFuture(handleListTools());
            case "tools/call" -> handleCallToolAsync(params);
            case "resources/list" -> Future.succeededFuture(handleListResources());
            case "resources/read" -> handleReadResource(params);
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
            .put("resources", new JsonObject().put("subscribe", false).put("listChanged", false))
            .put("prompts", new JsonObject().put("listChanged", false));

    JsonObject serverInfo =
        new JsonObject().put("name", "datasqrl-mcp-server").put("version", "0.7.0");

    return new JsonObject()
        .put("protocolVersion", PROTOCOL_VERSION)
        .put("capabilities", capabilities)
        .put("serverInfo", serverInfo);
  }

  private JsonObject handleListTools() {
    JsonArray toolsArray = new JsonArray();

    for (AsyncTool tool : tools.values()) {
      JsonObject toolInfo =
          new JsonObject().put("name", tool.name).put("description", tool.description);

      if (tool.inputSchema != null) {
        toolInfo.put("inputSchema", tool.inputSchema);
      }

      toolsArray.add(toolInfo);
    }

    return new JsonObject().put("tools", toolsArray);
  }

  private JsonObject handleListResources() {
    // Return a sample resource - server status information
    JsonArray resourcesArray = new JsonArray();

    //    JsonObject serverStatusResource = new JsonObject()
    //        .put("uri", "vertx://server/status")
    //        .put("name", "Server Status")
    //        .put("description", "Current server status and information")
    //        .put("mimeType", "application/json");
    //
    //    resourcesArray.add(serverStatusResource);

    return new JsonObject().put("resources", resourcesArray);
  }

  private Future<JsonObject> handleReadResource(JsonObject params) {
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

  private Future<JsonObject> handleCallToolAsync(JsonObject params) {
    String toolName = params.getString("name");
    JsonObject arguments = params.getJsonObject("arguments", new JsonObject());

    AsyncTool tool = tools.get(toolName);
    if (tool == null) {
      return Future.failedFuture(new McpException(-32602, "Tool not found: " + toolName));
    }

    return tool.handler
        .handle(arguments)
        .map(
            contentArray -> {
              // Return proper CallToolResult format
              JsonObject result = new JsonObject().put("content", contentArray);
              // Note: isError defaults to false and can be omitted for successful calls
              return result;
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

  // Sync tool example
  private Future<JsonArray> echoTool(JsonObject args) {
    String message = args.getString("message", "");
    JsonArray result =
        new JsonArray().add(new JsonObject().put("type", "text").put("text", "Echo: " + message));
    return Future.succeededFuture(result);
  }

  private JsonObject createResponse(Object id, JsonObject result) {
    return new JsonObject().put("jsonrpc", JSONRPC_VERSION).put("id", id).put("result", result);
  }

  private JsonObject createErrorResponse(Object id, int code, String message) {
    JsonObject error = new JsonObject().put("code", code).put("message", message);

    return new JsonObject().put("jsonrpc", JSONRPC_VERSION).put("id", id).put("error", error);
  }

  static class AsyncTool {
    final String name;
    final String description;
    final JsonObject inputSchema;
    final AsyncToolHandler handler;

    AsyncTool(String name, String description, JsonObject inputSchema, AsyncToolHandler handler) {
      this.name = name;
      this.description = description;
      this.inputSchema = inputSchema;
      this.handler = handler;
    }
  }

  @FunctionalInterface
  interface AsyncToolHandler {
    Future<JsonArray> handle(JsonObject arguments);
  }

  static class McpException extends RuntimeException {
    final int code;

    McpException(int code, String message) {
      super(message);
      this.code = code;
    }
  }
}
