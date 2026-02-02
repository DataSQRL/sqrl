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
package com.datasqrl.graphql.controller;

import com.datasqrl.graphql.config.ServerConfigProperties;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.operation.ApiOperation;
import com.datasqrl.graphql.server.operation.McpMethodType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/**
 * MCP (Model Context Protocol) bridge controller. Implements JSON-RPC 2.0 with SSE for
 * server-to-client streaming. Replaces the Vert.x-based McpBridgeVerticle.
 */
@Slf4j
@RestController
@RequiredArgsConstructor
public class McpBridgeController {

  private static final String JSONRPC_VERSION = "2.0";
  private static final String PROTOCOL_VERSION = "2024-11-05";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Map<String, GraphQL> graphQLEngines;
  private final Map<String, RootGraphqlModel> rootGraphqlModels;
  private final ServerConfigProperties config;

  private final ConcurrentHashMap<String, SseConnection> sseConnections = new ConcurrentHashMap<>();

  @PostMapping(
      value = "/mcp",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public Mono<ResponseEntity<Map<String, Object>>> handleMcpPost(
      @RequestBody JsonNode payload, Principal principal) {
    return handleMcpRequest("", payload, principal);
  }

  @PostMapping(
      value = "/{version}/mcp",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public Mono<ResponseEntity<Map<String, Object>>> handleVersionedMcpPost(
      @PathVariable String version, @RequestBody JsonNode payload, Principal principal) {
    return handleMcpRequest(version, payload, principal);
  }

  @GetMapping(value = "/mcp/sse", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<ServerSentEvent<String>> handleMcpSse() {
    return createSseFlux("");
  }

  @GetMapping(value = "/{version}/mcp/sse", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<ServerSentEvent<String>> handleVersionedMcpSse(@PathVariable String version) {
    return createSseFlux(version);
  }

  private Mono<ResponseEntity<Map<String, Object>>> handleMcpRequest(
      String version, JsonNode payload, Principal principal) {

    var graphQL = graphQLEngines.get(version);
    var model = rootGraphqlModels.get(version);

    if (graphQL == null || model == null) {
      return Mono.just(
          ResponseEntity.status(HttpStatus.NOT_FOUND)
              .body(createErrorResponse(null, -32601, "MCP not found for version: " + version)));
    }

    // Handle batch requests
    if (payload.isArray()) {
      return handleBatchRequest(version, payload, principal, graphQL, model);
    }

    return handleSingleRequest(version, payload, principal, graphQL, model);
  }

  private Mono<ResponseEntity<Map<String, Object>>> handleSingleRequest(
      String version,
      JsonNode payload,
      Principal principal,
      GraphQL graphQL,
      RootGraphqlModel model) {

    var method = payload.has("method") ? payload.get("method").asText() : null;
    var id = payload.has("id") ? payload.get("id") : null;
    var params = payload.has("params") ? payload.get("params") : null;

    log.debug("MCP request: method={}, id={}", method, id);

    if (method == null) {
      return Mono.just(
          ResponseEntity.ok(createErrorResponse(id, -32601, "Method not found: null")));
    }

    return switch (method) {
      case "initialize" -> handleInitialize(id);
      case "tools/list" -> handleToolsList(id, model);
      case "tools/call" -> handleToolCall(id, params, principal, graphQL, model);
      case "resources/list" -> handleResourcesList(id, model);
      case "resources/templates/list" -> handleResourceTemplatesList(id, model);
      case "resources/read" -> handleResourceRead(id, params, principal, graphQL, model);
      case "ping" -> handlePing(id);
      default ->
          Mono.just(
              ResponseEntity.ok(createErrorResponse(id, -32601, "Method not found: " + method)));
    };
  }

  private Mono<ResponseEntity<Map<String, Object>>> handleBatchRequest(
      String version,
      JsonNode payload,
      Principal principal,
      GraphQL graphQL,
      RootGraphqlModel model) {
    // For simplicity, just handle the first request in batch
    if (payload.size() > 0) {
      return handleSingleRequest(version, payload.get(0), principal, graphQL, model);
    }
    return Mono.just(
        ResponseEntity.badRequest().body(createErrorResponse(null, -32600, "Invalid Request")));
  }

  private Mono<ResponseEntity<Map<String, Object>>> handleInitialize(JsonNode id) {
    var result =
        Map.of(
            "protocolVersion", PROTOCOL_VERSION,
            "capabilities",
                Map.of(
                    "tools", Map.of("listChanged", false),
                    "resources", Map.of("subscribe", false, "listChanged", false)),
            "serverInfo",
                Map.of(
                    "name", "sqrl-server",
                    "version", "1.0.0"));
    return Mono.just(ResponseEntity.ok(createSuccessResponse(id, result)));
  }

  private Mono<ResponseEntity<Map<String, Object>>> handleToolsList(
      JsonNode id, RootGraphqlModel model) {
    var tools =
        model.getOperations().stream()
            .filter(op -> op.getMcpMethod() == McpMethodType.TOOL)
            .map(this::operationToTool)
            .toList();

    var result = Map.of("tools", tools);
    return Mono.just(ResponseEntity.ok(createSuccessResponse(id, result)));
  }

  private Mono<ResponseEntity<Map<String, Object>>> handleToolCall(
      JsonNode id, JsonNode params, Principal principal, GraphQL graphQL, RootGraphqlModel model) {

    if (params == null || !params.has("name")) {
      return Mono.just(
          ResponseEntity.ok(createErrorResponse(id, -32602, "Invalid params: missing tool name")));
    }

    var toolName = params.get("name").asText();
    var arguments = params.has("arguments") ? params.get("arguments") : null;

    var tools =
        model.getOperations().stream()
            .filter(op -> op.getMcpMethod() == McpMethodType.TOOL)
            .collect(Collectors.toMap(ApiOperation::getId, Function.identity()));

    var tool = tools.get(toolName);
    if (tool == null) {
      return Mono.just(
          ResponseEntity.ok(createErrorResponse(id, -32602, "Tool not found: " + toolName)));
    }

    Map<String, Object> variables = new HashMap<>();
    if (arguments != null) {
      try {
        variables =
            OBJECT_MAPPER.convertValue(
                arguments,
                new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
      } catch (Exception e) {
        return Mono.just(
            ResponseEntity.ok(
                createErrorResponse(id, -32602, "Invalid arguments: " + e.getMessage())));
      }
    }

    return executeGraphQL(graphQL, tool, variables, principal)
        .map(
            executionResult -> {
              if (!executionResult.getErrors().isEmpty()) {
                var errorMsg =
                    executionResult.getErrors().stream()
                        .map(err -> err.getMessage())
                        .collect(Collectors.joining("; "));
                return ResponseEntity.ok(createErrorResponse(id, -32603, errorMsg));
              }

              var data = getExecutionData(executionResult, tool);
              var content = List.of(Map.of("type", "text", "text", serializeResult(data)));
              var result = Map.of("content", content, "isError", false);
              return ResponseEntity.ok(createSuccessResponse(id, result));
            });
  }

  private Mono<ResponseEntity<Map<String, Object>>> handleResourcesList(
      JsonNode id, RootGraphqlModel model) {
    var resources =
        model.getOperations().stream()
            .filter(op -> op.getMcpMethod() == McpMethodType.RESOURCE)
            .filter(
                op ->
                    op.getFunction().getParameters() == null
                        || op.getFunction().getParameters().getProperties() == null
                        || op.getFunction().getParameters().getProperties().isEmpty())
            .map(this::operationToResource)
            .toList();

    var result = Map.of("resources", resources);
    return Mono.just(ResponseEntity.ok(createSuccessResponse(id, result)));
  }

  private Mono<ResponseEntity<Map<String, Object>>> handleResourceTemplatesList(
      JsonNode id, RootGraphqlModel model) {
    var templates =
        model.getOperations().stream()
            .filter(op -> op.getMcpMethod() == McpMethodType.RESOURCE)
            .filter(
                op ->
                    op.getFunction().getParameters() != null
                        && op.getFunction().getParameters().getProperties() != null
                        && !op.getFunction().getParameters().getProperties().isEmpty())
            .map(this::operationToResourceTemplate)
            .toList();

    var result = Map.of("resourceTemplates", templates);
    return Mono.just(ResponseEntity.ok(createSuccessResponse(id, result)));
  }

  private Mono<ResponseEntity<Map<String, Object>>> handleResourceRead(
      JsonNode id, JsonNode params, Principal principal, GraphQL graphQL, RootGraphqlModel model) {

    if (params == null || !params.has("uri")) {
      return Mono.just(
          ResponseEntity.ok(createErrorResponse(id, -32602, "Invalid params: missing uri")));
    }

    var uri = params.get("uri").asText();
    // Find matching resource
    var resources =
        model.getOperations().stream()
            .filter(op -> op.getMcpMethod() == McpMethodType.RESOURCE)
            .toList();

    for (var resource : resources) {
      if (matchesResourceUri(resource, uri)) {
        Map<String, Object> variables = extractUriParameters(resource, uri);

        return executeGraphQL(graphQL, resource, variables, principal)
            .map(
                executionResult -> {
                  if (!executionResult.getErrors().isEmpty()) {
                    var errorMsg =
                        executionResult.getErrors().stream()
                            .map(err -> err.getMessage())
                            .collect(Collectors.joining("; "));
                    return ResponseEntity.ok(createErrorResponse(id, -32603, errorMsg));
                  }

                  var data = getExecutionData(executionResult, resource);
                  var content =
                      List.of(
                          Map.of(
                              "uri",
                              uri,
                              "mimeType",
                              "application/json",
                              "text",
                              serializeResult(data)));
                  var result = Map.of("contents", content);
                  return ResponseEntity.ok(createSuccessResponse(id, result));
                });
      }
    }

    return Mono.just(
        ResponseEntity.ok(createErrorResponse(id, -32602, "Resource not found: " + uri)));
  }

  private Mono<ResponseEntity<Map<String, Object>>> handlePing(JsonNode id) {
    return Mono.just(ResponseEntity.ok(createSuccessResponse(id, Map.of())));
  }

  private Flux<ServerSentEvent<String>> createSseFlux(String version) {
    var sessionId = UUID.randomUUID().toString();
    var sink = Sinks.many().multicast().<String>onBackpressureBuffer();

    var connection = new SseConnection(sessionId, sink);
    sseConnections.put(sessionId, connection);

    log.info("New SSE connection: {}", sessionId);

    // Send endpoint event
    var mcpEndpoint = config.getServletConfig().getMcpEndpoint(version);
    sink.tryEmitNext("{\"sessionId\":\"" + sessionId + "\",\"endpoint\":\"" + mcpEndpoint + "\"}");

    return sink.asFlux()
        .map(data -> ServerSentEvent.<String>builder().event("message").data(data).build())
        .doOnCancel(
            () -> {
              sseConnections.remove(sessionId);
              log.info("SSE connection closed: {}", sessionId);
            });
  }

  private Map<String, Object> operationToTool(ApiOperation op) {
    var tool = new HashMap<String, Object>();
    tool.put("name", op.getId());
    var description = op.getFunction().getDescription();
    tool.put("description", description != null ? description : op.getName());

    if (op.getFunction().getParameters() != null) {
      tool.put("inputSchema", op.getFunction().getParameters());
    } else {
      tool.put("inputSchema", Map.of("type", "object", "properties", Map.of()));
    }

    return tool;
  }

  private Map<String, Object> operationToResource(ApiOperation op) {
    var resource = new HashMap<String, Object>();
    resource.put("uri", op.getUriTemplate());
    resource.put("name", op.getName());
    resource.put("description", op.getFunction().getDescription());
    resource.put("mimeType", "application/json");
    return resource;
  }

  private Map<String, Object> operationToResourceTemplate(ApiOperation op) {
    var template = new HashMap<String, Object>();
    template.put("uriTemplate", op.getUriTemplate());
    template.put("name", op.getName());
    template.put("description", op.getFunction().getDescription());
    template.put("mimeType", "application/json");
    return template;
  }

  private boolean matchesResourceUri(ApiOperation resource, String uri) {
    var template = resource.getUriTemplate();
    if (template == null) return false;
    var pattern = template.replaceAll("\\{[^}]+}", "[^/]+");
    return uri.matches(pattern);
  }

  private Map<String, Object> extractUriParameters(ApiOperation resource, String uri) {
    // Simple extraction - could be improved
    return Map.of();
  }

  private Mono<ExecutionResult> executeGraphQL(
      GraphQL graphQL, ApiOperation operation, Map<String, Object> variables, Principal principal) {

    var executionInput =
        ExecutionInput.newExecutionInput()
            .query(operation.getApiQuery().query())
            .operationName(operation.getApiQuery().queryName())
            .variables(variables)
            .graphQLContext(
                builder -> {
                  if (principal instanceof Authentication auth
                      && auth.getPrincipal() instanceof Jwt jwt) {
                    builder.put("claims", jwt.getClaims());
                  }
                })
            .build();

    return Mono.fromFuture(graphQL.executeAsync(executionInput));
  }

  private static Object getExecutionData(ExecutionResult executionResult, ApiOperation operation) {
    var result = executionResult.getData();
    if (result instanceof Map<?, ?> resultMap && operation.removeNesting()) {
      if (resultMap.size() == 1) {
        result = resultMap.values().iterator().next();
      }
    }
    return result;
  }

  private String serializeResult(Object data) {
    try {
      return OBJECT_MAPPER.writeValueAsString(data);
    } catch (Exception e) {
      return String.valueOf(data);
    }
  }

  private Map<String, Object> createSuccessResponse(JsonNode id, Object result) {
    var response = new HashMap<String, Object>();
    response.put("jsonrpc", JSONRPC_VERSION);
    if (id != null) {
      response.put("id", id.isTextual() ? id.asText() : id.asInt());
    }
    response.put("result", result);
    return response;
  }

  private Map<String, Object> createErrorResponse(JsonNode id, int code, String message) {
    var response = new HashMap<String, Object>();
    response.put("jsonrpc", JSONRPC_VERSION);
    if (id != null) {
      response.put("id", id.isTextual() ? id.asText() : id.asInt());
    }
    response.put("error", Map.of("code", code, "message", message));
    return response;
  }

  private record SseConnection(String sessionId, Sinks.Many<String> sink) {}
}
