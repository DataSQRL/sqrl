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

import com.datasqrl.server.config.ServerConfig;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.operation.ApiOperation;
import com.datasqrl.server.operation.FunctionDefinition;
import com.datasqrl.util.JsonUtils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.Error;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.dialect.Dialects;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Abstract Verticle that maps requests to GraphQL queries */
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
@Slf4j
public abstract class AbstractBridgeVerticle extends AbstractVerticle {

  // Reusable JSON field names
  protected static final String JSON_ERROR = "error";
  protected static final String JSON_MESSAGE = "message";

  protected final Router router;
  protected final ServerConfig config;
  protected final String modelVersion;
  protected final RootGraphQLModel model;
  protected final List<AuthenticationProvider> authProviders;
  protected final GraphQLServerVerticle graphQLServerVerticle;

  protected void handleError(
      Throwable err, RoutingContext ctx, int statusCode, String errorMessage) {
    if (statusCode == 500) {
      log.error(errorMessage, err);
    } else {
      log.info(errorMessage, err);
    }

    ctx.response()
        .setStatusCode(statusCode)
        .putHeader("content-type", "application/json")
        .end(
            new JsonObject()
                .put(JSON_ERROR, errorMessage)
                .put(JSON_MESSAGE, err.getMessage())
                .encode());
  }

  protected Future<ExecutionResult> bridgeRequestToGraphQL(
      RoutingContext ctx, ApiOperation operation, Map<String, Object> variables) {
    // Validate parameters
    validateParameters(variables, operation);

    // Execute GraphQL query directly with ExecutionInput
    return executeGraphQLAsync(ctx, operation, variables);
  }

  protected void validateParameters(Map<String, Object> variables, ApiOperation operation) {
    var parameters = operation.getFunction().getParameters();
    if (parameters == null) {
      return; // No validation required
    }
    final JsonNode arguments;
    final Schema schema;
    try {
      // Build a JSON Schema from the parameters definition
      var schemaText = getSchemaMapper().writeValueAsString(parameters);
      var schemaRegistry = SchemaRegistry.withDefaultDialect(Dialects.getDraft202012());
      schema = schemaRegistry.getSchema(schemaText);

      // Convert the collected variables to a JsonNode
      if (variables == null || variables.isEmpty()) {
        arguments = JsonUtils.MAPPER.readTree("{}");
      } else {
        arguments = JsonUtils.MAPPER.valueToTree(variables);
      }
    } catch (JsonProcessingException e) {
      throw new ValidationException("Could not parse parameter JSON:" + e.getMessage());
    }

    // Validate against the schema
    var schemaErrors = schema.validate(arguments);
    if (!schemaErrors.isEmpty()) {
      var schemaErrorsText =
          schemaErrors.stream().map(Error::toString).collect(Collectors.joining("; "));
      log.info("Function call had schema errors: {}", schemaErrorsText);
      throw new ValidationException("Invalid Schema: " + schemaErrorsText);
    }
  }

  protected ObjectMapper getSchemaMapper() {
    return JsonUtils.MAPPER.copy().setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
  }

  protected Future<ExecutionResult> executeGraphQLAsync(
      RoutingContext ctx, ApiOperation operation, Map<String, Object> variables) {

    var graphQLEngine = graphQLServerVerticle.getGraphQLEngine();

    // Build the ExecutionInput
    var execInput =
        ExecutionInput.newExecutionInput()
            .query(operation.getApiQuery().query())
            .operationName(operation.getApiQuery().queryName())
            .variables(variables)
            .graphQLContext(builder -> builder.put(RoutingContext.class, ctx))
            .build();

    // Kick off async execution (GraphQL Java spawns its own executor)
    return Future.fromCompletionStage(graphQLEngine.executeAsync(execInput));
  }

  protected static Object getExecutionData(
      ExecutionResult executionResult, ApiOperation operation) {
    var result = executionResult.getData();
    if (result instanceof Map resultMap && operation.removeNesting()) {
      if (resultMap.size() == 1) {
        result = resultMap.values().iterator().next(); // Get only element
      }
    }
    return result;
  }

  protected static void extractUriParameters(
      RoutingContext ctx, ApiOperation operation, Map<String, Object> variables) {
    var request = ctx.request();
    var functionParams = operation.getFunction().getParameters();

    if (functionParams == null || functionParams.getProperties() == null) {
      return;
    }

    var properties = functionParams.getProperties();
    var queryParams = request.params();

    for (String key : queryParams.names()) {
      var argument = properties.get(key);
      if (argument != null) {
        variables.put(key, convertParameterValue(queryParams.get(key), argument));
      }
    }

    // Merge query and path parameters, giving precedence to path params
    extractPathParameters(ctx, operation, variables);
  }

  protected static void extractPathParameters(
      RoutingContext ctx, ApiOperation operation, Map<String, Object> variables) {
    var functionParams = operation.getFunction().getParameters();
    if (functionParams == null || functionParams.getProperties() == null) {
      return;
    }

    var properties = functionParams.getProperties();
    for (var pathParameter : ctx.pathParams().entrySet()) {
      var argument = properties.get(pathParameter.getKey());
      if (argument != null) {
        variables.put(
            pathParameter.getKey(), convertParameterValue(pathParameter.getValue(), argument));
      }
    }
  }

  protected static void extractBodyParameters(RoutingContext ctx, Map<String, Object> variables) {
    JsonObject body = ctx.body().asJsonObject();
    if (body != null) {
      for (var parameter : body.getMap().entrySet()) {
        var name = parameter.getKey();
        var bodyValue = parameter.getValue();
        if (variables.containsKey(name)
            && !String.valueOf(variables.get(name)).equals(String.valueOf(bodyValue))) {
          throw new ValidationException(
              "Path parameter '%s' does not match the request body".formatted(name));
        }
        variables.put(name, bodyValue);
      }
    }
  }

  protected static Object convertParameterValue(
      String value, FunctionDefinition.Argument argumentDef) {
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

  /** Custom exception for parameter validation errors */
  public static class ValidationException extends RuntimeException {
    public ValidationException(String message) {
      super(message);
    }
  }
}
