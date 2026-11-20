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
import com.datasqrl.server.operation.McpApp;
import com.datasqrl.server.operation.McpVisibility;
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
import io.modelcontextprotocol.server.McpStatelessServerFeatures.AsyncResourceSpecification;
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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

/**
 * Exposes the operations that are MCP tools over the Model Context Protocol, and executes each tool
 * call as its GraphQL operation. The protocol is handled by the stateless server of the MCP Java
 * SDK, which {@link VertxMcpTransport} serves on the MCP endpoint.
 *
 * <p>With MCP Apps support, it also serves the MCP apps of the model as resources, from their files
 * in the plan directory, and links the tools to them as the MCP Apps extension ({@value
 * McpApp#EXTENSION_ID}) defines.
 */
@Slf4j
public class McpBridgeVerticle extends AbstractBridgeVerticle {

  private static final String SERVER_NAME = "datasqrl-mcp-server";

  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  /** The visibility of a tool that does not set one: the default of the MCP Apps extension. */
  private static final List<McpVisibility> DEFAULT_VISIBILITY =
      List.of(McpVisibility.MODEL, McpVisibility.APP);

  private final List<ApiOperation> tools;

  /** The MCP apps by name, or null if the model has no MCP Apps support. */
  private final Map<String, McpApp> apps;

  /** The plan directory, which has the files of the MCP apps. */
  private final Path planDir;

  private McpStatelessAsyncServer mcpServer;

  public McpBridgeVerticle(
      Router router,
      ServerConfig config,
      String modelVersion,
      RootGraphQLModel model,
      List<AuthenticationProvider> authProviders,
      GraphQLServerVerticle graphQLServerVerticle,
      Path planDir) {
    super(router, config, modelVersion, model, authProviders, graphQLServerVerticle);
    this.planDir = planDir;
    this.tools = model.getOperations().stream().filter(ApiOperation::isMcpEndpoint).toList();
    this.apps =
        model.getMcpApps() == null
            ? null
            : model.getMcpApps().stream()
                .collect(
                    Collectors.toMap(
                        McpApp::name,
                        Function.identity(),
                        (first, second) -> first,
                        LinkedHashMap::new));
  }

  @Override
  public void start(Promise<Void> startPromise) {
    var endpoint = config.getServletConfig().getMcpEndpoint(modelVersion);
    var jsonMapper = new JacksonMcpJsonMapper(new ObjectMapper());
    var transport = new VertxMcpTransport(jsonMapper);
    try {
      var capabilities = McpSchema.ServerCapabilities.builder().tools(false);
      var server =
          McpServer.async(transport)
              .jsonMapper(jsonMapper)
              .serverInfo(SERVER_NAME, ProjectConstants.SQRL_VERSION)
              // The bridge validates the arguments, once it has removed optional nulls
              .validateToolInputs(false)
              .tools(tools.stream().map(this::toolSpecification).toList());
      if (apps != null) {
        capabilities
            .resources(false, false)
            .extensions(
                Map.of(McpApp.EXTENSION_ID, Map.of("mimeTypes", List.of(McpApp.MIME_TYPE))));
        server.resources(apps.values().stream().map(this::resourceSpecification).toList());
      }
      mcpServer = server.capabilities(capabilities.build()).build();
    } catch (RuntimeException e) {
      startPromise.fail(e);
      return;
    }

    var postRoute = router.post(endpoint);
    if (!authProviders.isEmpty()) {
      postRoute.handler(AuthHandlerFactory.create(authProviders));
      postRoute.failureHandler(
          new OAuthFailureHandler(config.getProtectedResourceMetadata().orElse(null)));
    }
    postRoute.handler(transport::handlePost);
    router.get(endpoint).handler(transport::handleGet);

    log.info(
        "MCP endpoint {} serves {} tool(s) and {} app(s) with {} authentication provider(s)",
        endpoint,
        tools.size(),
        apps == null ? 0 : apps.size(),
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
        McpSchema.Tool.builder(operation.getName(), inputSchema)
            .description(description)
            .annotations(
                McpSchema.ToolAnnotations.builder()
                    .readOnlyHint(operationType != Operation.MUTATION)
                    .build())
            .meta(toolMeta(operation))
            .build();
    return new AsyncToolSpecification(
        tool, (transportContext, request) -> callTool(transportContext, operation, request));
  }

  /**
   * Returns the {@code _meta} of the tool with the MCP Apps fields: the {@code resourceUri} of its
   * app, and its {@code visibility}. Without MCP Apps support, only a tool that sets a visibility
   * has them.
   */
  private Map<String, Object> toolMeta(ApiOperation operation) {
    var ui = new LinkedHashMap<String, Object>();
    if (operation.getAppName() != null && apps != null) {
      var app = apps.get(operation.getAppName());
      if (app != null) {
        ui.put("resourceUri", app.uri());
      } else {
        log.warn(
            "MCP tool {} is linked to the unknown app {}",
            operation.getName(),
            operation.getAppName());
      }
    }
    var visibility = operation.getMcpVisibility();
    if (visibility == null && apps != null) {
      visibility = DEFAULT_VISIBILITY;
    }
    if (visibility != null) {
      ui.put(
          "visibility",
          visibility.stream().map(value -> value.name().toLowerCase(Locale.ROOT)).toList());
    }
    return ui.isEmpty() ? null : Map.of("ui", ui);
  }

  /** Serves the app as a resource, with the content of its file, which is read once. */
  private AsyncResourceSpecification resourceSpecification(McpApp app) {
    var resource =
        McpSchema.Resource.builder(app.uri(), app.name()).mimeType(app.mimeType()).build();

    var textContent =
        McpSchema.TextResourceContents.builder(app.uri(), readFile(app))
            .mimeType(app.mimeType())
            .build();

    var contents = McpSchema.ReadResourceResult.builder(List.of(textContent)).build();

    return new AsyncResourceSpecification(
        resource, (transportContext, request) -> Mono.just(contents));
  }

  private String readFile(McpApp app) {
    var file = planDir.resolve(app.file());
    try {
      return Files.readString(file);
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Could not read the file %s of the MCP app %s".formatted(file, app.name()), e);
    }
  }

  private Mono<CallToolResult> callTool(
      McpTransportContext transportContext, ApiOperation operation, CallToolRequest request) {

    var ctx = (RoutingContext) transportContext.get(VertxMcpTransport.ROUTING_CONTEXT_KEY);
    // A call of a tool without required arguments may leave out the arguments
    var arguments = request.arguments() == null ? Map.<String, Object>of() : request.arguments();

    return Mono.defer(
            () ->
                Mono.fromCompletionStage(
                    bridgeRequestToGraphQL(ctx, operation, arguments).toCompletionStage()))
        .map(result -> toToolResult(result, operation))
        .onErrorResume(error -> Mono.just(toErrorResult(operation, error)));
  }

  /** Returns the GraphQL errors, or else the data, as the tool result. */
  private static CallToolResult toToolResult(ExecutionResult result, ApiOperation operation) {
    if (!result.getErrors().isEmpty()) {
      var errors =
          result.getErrors().stream()
              .<Content>map(
                  error ->
                      TextContent.builder(
                              "Tool Error[" + error.getPath() + "]: " + error.getMessage())
                          .build())
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
