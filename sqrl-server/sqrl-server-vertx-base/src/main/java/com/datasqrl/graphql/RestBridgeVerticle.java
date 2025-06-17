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

import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.operation.ApiOperation;
import com.datasqrl.graphql.server.operation.FunctionDefinition;
import com.datasqrl.graphql.server.operation.HttpMethod;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.JWTAuthHandler;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

/**
 * REST-to-GraphQL bridge verticle that creates REST endpoints for API operations with
 * restEndpoint=true. Handles URI template expansion, parameter validation, and GraphQL query
 * routing.
 */
@Slf4j
public class RestBridgeVerticle extends AbstractVerticle {

  private final Router router;
  private final ServerConfig config;
  private final RootGraphqlModel model;
  private final Optional<JWTAuth> jwtAuth;
  private final ObjectMapper objectMapper;
  private final GraphQLServerVerticle graphQLServerVerticle;

  // Pattern for RFC 6570 URI template query parameters: {?param1,param2}
  private static final Pattern QUERY_PARAMS_PATTERN = Pattern.compile("\\{\\?([^}]+)\\}");
  // Pattern for RFC 6570 URI template path parameters: {param}
  private static final Pattern PATH_PARAMS_PATTERN = Pattern.compile("\\{([^}?]+)\\}");

  // Reusable JSON field names
  private static final String JSON_ERROR = "error";
  private static final String JSON_DETAILS = "details";
  private static final String JSON_MESSAGE = "message";

  public RestBridgeVerticle(
      Router router,
      ServerConfig config,
      RootGraphqlModel model,
      Optional<JWTAuth> jwtAuth,
      GraphQLServerVerticle graphQLServerVerticle) {
    this.router = router;
    this.config = config;
    this.model = model;
    this.jwtAuth = jwtAuth;
    this.objectMapper = new ObjectMapper();
    this.graphQLServerVerticle = graphQLServerVerticle;
  }

  @Override
  public void start(Promise<Void> startPromise) {
    try {
      setupRestEndpoints();
      startPromise.complete();
    } catch (Exception e) {
      log.error("Could not setup REST endpoints", e);
      startPromise.fail(e);
    }
  }

  private void setupRestEndpoints() {
    // Process each API operation and create REST endpoints for those with restEndpoint=true
    for (ApiOperation operation : model.getOperations()) {
      if (operation.isRestEndpoint()) {
        createRestEndpoint(operation);
      }
    }
  }

  private void createRestEndpoint(ApiOperation operation) {
    String uriTemplate = operation.getUriTemplate();
    HttpMethod httpMethod = operation.getHttpMethod();

    if (uriTemplate == null || httpMethod == null) {
      log.warn(
          "Skipping REST endpoint for operation {} - missing uriTemplate or httpMethod",
          operation.getName());
      return;
    }

    // Convert URI template to Vert.x route pattern
    String routePattern = convertUriTemplateToRoute(uriTemplate);

    log.info(
        "Creating REST endpoint: {} {} -> GraphQL {}",
        httpMethod,
        routePattern,
        operation.getName());

    // Create route based on HTTP method
    var route =
        switch (httpMethod) {
          case GET -> router.get(routePattern);
          case POST -> router.post(routePattern);
          case PUT -> throw new UnsupportedOperationException("PUT not yet supported");
        };

    // Add JWT auth if configured
    jwtAuth.ifPresent(auth -> route.handler(JWTAuthHandler.create(auth)));

    // Add the REST handler
    route.handler(ctx -> handleRestRequest(ctx, operation));
  }

  /**
   * Converts RFC 6570 URI template to Vert.x route pattern. Examples: -
   * "queries/HighTempAlert{?offset,limit}" -> "/queries/HighTempAlert" - "mutations/SensorReading"
   * -> "/mutations/SensorReading" - "users/{userId}/posts" -> "/users/:userId/posts"
   */
  private String convertUriTemplateToRoute(String uriTemplate) {
    // Remove query parameters pattern {?param1,param2}
    String route = QUERY_PARAMS_PATTERN.matcher(uriTemplate).replaceAll("");

    // Convert path parameters {param} to :param
    route = PATH_PARAMS_PATTERN.matcher(route).replaceAll(":$1");

    // Ensure route starts with /
    if (!route.startsWith("/")) {
      route = "/" + route;
    }
    // Ensure route starts with REST prefix
    route = config.getServletConfig().getRestEndpoint() + route;
    return route;
  }

  private void handleRestRequest(RoutingContext ctx, ApiOperation operation) {
    try {
      // Extract parameters directly into a Map
      Map<String, Object> variables = extractParameters(ctx, operation);

      // Validate parameters
      validateParameters(variables, operation);

      // Execute GraphQL query directly with ExecutionInput
      executeGraphQLAsync(ctx, operation, variables);

    } catch (ValidationException e) {
      log.warn(
          "Parameter validation failed for operation {}: {}", operation.getName(), e.getMessage());
      ctx.response()
          .setStatusCode(400)
          .putHeader("content-type", "application/json")
          .end(
              new JsonObject()
                  .put(JSON_ERROR, "Parameter validation failed")
                  .put(JSON_DETAILS, e.getMessage())
                  .encode());
    } catch (Exception e) {
      log.error("Error handling REST request for operation " + operation.getName(), e);
      ctx.response()
          .setStatusCode(500)
          .putHeader("content-type", "application/json")
          .end(new JsonObject().put(JSON_ERROR, "Internal server error").encode());
    }
  }

  private Map<String, Object> extractParameters(RoutingContext ctx, ApiOperation operation)
      throws Exception {
    Map<String, Object> variables = new HashMap<>();

    if (operation.getHttpMethod() == HttpMethod.GET) {
      // For GET requests, extract parameters from URL query parameters and path parameters
      extractGetParameters(ctx, operation, variables);
    } else {
      // For POST/PUT requests, use the JSON body as variables
      extractPostParameters(ctx, variables);
    }

    return variables;
  }

  private void extractGetParameters(
      RoutingContext ctx, ApiOperation operation, Map<String, Object> variables) {
    HttpServerRequest request = ctx.request();
    FunctionDefinition.Parameters functionParams = operation.getFunction().getParameters();

    // Early return if no parameters defined
    if (functionParams == null || functionParams.getProperties() == null) {
      return;
    }

    Map<String, FunctionDefinition.Argument> properties = functionParams.getProperties();

    // Extract query parameters
    MultiMap queryParams = request.params();
    for (String paramName : properties.keySet()) {
      String value = queryParams.get(paramName);
      if (value != null) {
        Object convertedValue = convertParameterValue(value, properties.get(paramName));
        variables.put(paramName, convertedValue);
      }
    }

    // Extract path parameters
    Map<String, String> pathParams = ctx.pathParams();
    for (Map.Entry<String, String> entry : pathParams.entrySet()) {
      String paramName = entry.getKey();
      String value = entry.getValue();
      if (properties.containsKey(paramName)) {
        Object convertedValue = convertParameterValue(value, properties.get(paramName));
        variables.put(paramName, convertedValue);
      }
    }
  }

  private void extractPostParameters(RoutingContext ctx, Map<String, Object> variables)
      throws Exception {
    Buffer body = ctx.body().buffer();
    if (body != null && body.length() > 0) {
      @SuppressWarnings("unchecked")
      Map<String, Object> bodyMap = objectMapper.readValue(body.getBytes(), Map.class);
      variables.putAll(bodyMap);
    }
  }

  private Object convertParameterValue(String value, FunctionDefinition.Argument argumentDef) {
    if (argumentDef == null || argumentDef.getType() == null) {
      return value;
    }

    return switch (argumentDef.getType()) {
      case "integer" -> {
        try {
          yield Long.parseLong(value);
        } catch (NumberFormatException e) {
          yield value; // Let validation catch this
        }
      }
      case "number" -> {
        try {
          yield Double.parseDouble(value);
        } catch (NumberFormatException e) {
          yield value; // Let validation catch this
        }
      }
      case "boolean" -> Boolean.parseBoolean(value);
      default -> value;
    };
  }

  private void validateParameters(Map<String, Object> variables, ApiOperation operation)
      throws ValidationException {
    FunctionDefinition.Parameters parameters = operation.getFunction().getParameters();
    if (parameters == null) {
      return; // No validation needed
    }

    // Check required parameters
    if (parameters.getRequired() != null) {
      for (String requiredParam : parameters.getRequired()) {
        if (!variables.containsKey(requiredParam) || variables.get(requiredParam) == null) {
          throw new ValidationException("Required parameter '" + requiredParam + "' is missing");
        }
      }
    }

    // Basic type validation
    if (parameters.getProperties() != null) {
      for (Map.Entry<String, FunctionDefinition.Argument> entry :
          parameters.getProperties().entrySet()) {
        String paramName = entry.getKey();
        FunctionDefinition.Argument argDef = entry.getValue();

        if (variables.containsKey(paramName)) {
          Object value = variables.get(paramName);
          if (value != null && !isValidType(value, argDef.getType())) {
            throw new ValidationException(
                "Parameter '" + paramName + "' has invalid type. Expected: " + argDef.getType());
          }
        }
      }
    }
  }

  private boolean isValidType(Object value, String expectedType) {
    if (expectedType == null) {
      return true;
    }

    return switch (expectedType) {
      case "integer" -> value instanceof Integer || value instanceof Long;
      case "number" -> value instanceof Number;
      case "string" -> value instanceof String;
      case "boolean" -> value instanceof Boolean;
      case "array" -> value instanceof java.util.List;
      case "object" -> value instanceof Map || value instanceof JsonObject;
      default -> true; // Unknown type, assume valid
    };
  }

  private void executeGraphQLAsync(
      RoutingContext ctx, ApiOperation operation, Map<String, Object> variables) {

    GraphQL graphQLEngine = graphQLServerVerticle.getGraphQLEngine();

    // Build the ExecutionInput
    ExecutionInput execInput =
        ExecutionInput.newExecutionInput()
            .query(operation.getApiQuery().query())
            .operationName(operation.getApiQuery().queryName())
            .variables(variables)
            .build();

    // 1️⃣ Kick off async execution (GraphQL Java spawns its own executor)
    Future<ExecutionResult> fut = Future.fromCompletionStage(graphQLEngine.executeAsync(execInput));

    // 2️⃣ Map the result back to the Vert.x context
    fut.onSuccess(
            executionResult -> {
              JsonObject json = new JsonObject().put("data", executionResult.getData());

              if (!executionResult.getErrors().isEmpty()) {
                json.put(
                    "errors",
                    executionResult.getErrors().stream()
                        .map(
                            err ->
                                new JsonObject()
                                    .put(JSON_MESSAGE, err.getMessage())
                                    .put("path", err.getPath())
                                    .put("extensions", err.getExtensions()))
                        .toList());
              }

              ctx.response()
                  .setStatusCode(200)
                  .putHeader("content-type", "application/json")
                  .end(json.encode());
            })
        .onFailure(
            err -> {
              log.error("GraphQL execution failed", err);
              ctx.response()
                  .setStatusCode(500)
                  .end(
                      new JsonObject()
                          .put(JSON_ERROR, "Failed to execute GraphQL query")
                          .put(JSON_MESSAGE, err.getMessage())
                          .encode());
            });
  }

  /** Custom exception for parameter validation errors */
  private static class ValidationException extends Exception {
    public ValidationException(String message) {
      super(message);
    }
  }
}
