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

import com.datasqrl.server.auth.AuthHandlerFactory;
import com.datasqrl.server.auth.OAuthFailureHandler;
import com.datasqrl.server.config.ServerConfig;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.mcp.VertxMcpTransport;
import com.datasqrl.server.operation.ApiOperation;
import com.datasqrl.server.operation.McpMethodType;
import com.datasqrl.util.JsonUtils;
import com.datasqrl.util.ProjectConstants;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.ExecutionResult;
import graphql.language.OperationDefinition.Operation;
import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpStatelessAsyncServer;
import io.modelcontextprotocol.server.McpStatelessServerFeatures.AsyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.Content;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.vertx.core.Promise;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

/**
 * Exposes the operations that are MCP tools over the Model Context Protocol, and executes each tool
 * call as its GraphQL operation. The protocol is handled by the stateless server of the MCP Java
 * SDK, which {@link VertxMcpTransport} serves on the MCP endpoint.
 */
@Slf4j
public class McpBridgeVerticle extends AbstractBridgeVerticle {

  private static final String SERVER_NAME = "datasqrl-mcp-server";

  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  private final List<ApiOperation> tools;
  private McpStatelessAsyncServer mcpServer;

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
            .toList();
  }

  @Override
  public void start(Promise<Void> startPromise) {
    var endpoint = config.getServletConfig().getMcpEndpoint(modelVersion);
    var jsonMapper = new JacksonMcpJsonMapper(new ObjectMapper());
    var transport = new VertxMcpTransport(jsonMapper);
    try {
      mcpServer =
          McpServer.async(transport)
              .jsonMapper(jsonMapper)
              .serverInfo(SERVER_NAME, ProjectConstants.SQRL_VERSION)
              .capabilities(McpSchema.ServerCapabilities.builder().tools(false).build())
              // The bridge validates the arguments, once it has removed optional nulls
              .validateToolInputs(false)
              .tools(tools.stream().map(this::toolSpecification).toList())
              .build();
    } catch (RuntimeException e) {
      startPromise.fail(e);
      return;
    }

    var postRoute = router.post(endpoint);
    if (!authProviders.isEmpty()) {
      postRoute.handler(AuthHandlerFactory.create(vertx, authProviders));
      postRoute.failureHandler(
          new OAuthFailureHandler(config.getProtectedResourceMetadata().orElse(null)));
    }
    postRoute.handler(transport::handlePost);
    router.get(endpoint).handler(transport::handleGet);

    log.info(
        "MCP endpoint {} serves {} tool(s) with {} authentication provider(s)",
        endpoint,
        tools.size(),
        authProviders.size());
    startPromise.complete();
  }

  @Override
  public void stop() {
    if (mcpServer != null) {
      mcpServer.close();
    }
  }

  private AsyncToolSpecification toolSpecification(ApiOperation operation) {
    var function = operation.getFunction();
    var operationType = operation.getApiQuery().operationType();
    var description = function.getDescription();
    if (description == null) {
      description =
          "Invokes %s %s".formatted(function.getName(), operationType.name().toLowerCase());
    }
    Map<String, Object> inputSchema =
        function.getParameters() == null
            ? Map.of("type", "object")
            : getSchemaMapper().convertValue(function.getParameters(), MAP_TYPE);
    var tool =
        McpSchema.Tool.builder()
            .name(operation.getName())
            .description(description)
            .inputSchema(inputSchema)
            .annotations(
                McpSchema.ToolAnnotations.builder()
                    .readOnlyHint(operationType != Operation.MUTATION)
                    .build())
            .build();
    return new AsyncToolSpecification(
        tool, (transportContext, request) -> callTool(transportContext, operation, request));
  }

  private Mono<CallToolResult> callTool(
      McpTransportContext transportContext, ApiOperation operation, CallToolRequest request) {
    var ctx = (RoutingContext) transportContext.get(VertxMcpTransport.ROUTING_CONTEXT_KEY);
    return Mono.defer(
            () ->
                Mono.fromCompletionStage(
                    bridgeRequestToGraphQL(ctx, operation, request.arguments())
                        .toCompletionStage()))
        .map(result -> toToolResult(result, operation))
        .onErrorResume(error -> Mono.just(toErrorResult(operation, error)));
  }

  /** Returns the GraphQL errors, or else the data, as the tool result. */
  private static CallToolResult toToolResult(ExecutionResult result, ApiOperation operation) {
    if (!result.getErrors().isEmpty()) {
      List<Content> errors =
          result.getErrors().stream()
              .<Content>map(
                  error ->
                      new TextContent("Tool Error[" + error.getPath() + "]: " + error.getMessage()))
              .toList();
      return CallToolResult.builder().content(errors).isError(true).build();
    }
    final String data;
    try {
      data = JsonUtils.MAPPER.writeValueAsString(getExecutionData(result, operation));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Could not serialize the result: " + e.getMessage(), e);
    }
    return CallToolResult.builder().addTextContent(data).isError(false).build();
  }

  /**
   * Returns a failed tool call as a tool result, not a protocol error, so that the model sees why
   * it failed.
   */
  private static CallToolResult toErrorResult(ApiOperation operation, Throwable error) {
    final String text;
    if (error instanceof ValidationException) {
      text = "Validation error: " + error.getMessage();
    } else {
      log.warn("MCP tool {} failed", operation.getName(), error);
      text = "Tool error: " + error.getMessage();
    }
    return CallToolResult.builder().addTextContent(text).isError(true).build();
  }
}
