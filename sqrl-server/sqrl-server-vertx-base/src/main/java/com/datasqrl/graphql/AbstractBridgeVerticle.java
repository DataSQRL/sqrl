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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Abstract Verticle that maps requests to GraphQL queries */
@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
@Slf4j
public abstract class AbstractBridgeVerticle extends AbstractVerticle {

  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Reusable JSON field names
  protected static final String JSON_ERROR = "error";
  protected static final String JSON_MESSAGE = "message";

  protected final Router router;
  protected final ServerConfig config;
  protected final String modelVersion;
  protected final RootGraphqlModel model;
  protected final Optional<AuthenticationProvider> authProvider;
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
      RoutingContext ctx, ApiOperation operation, Map<String, Object> variables)
      throws ValidationException {
    // Validate parameters
    validateParameters(variables, operation);

    // Execute GraphQL query directly with ExecutionInput
    return executeGraphQLAsync(ctx, operation, variables);
  }

  protected void validateParameters(Map<String, Object> variables, ApiOperation operation)
      throws ValidationException {
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
        arguments = OBJECT_MAPPER.readTree("{}");
      } else {
        arguments = OBJECT_MAPPER.valueToTree(variables);
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
    return OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
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

  protected static void extractGetParameters(
      RoutingContext ctx, ApiOperation operation, Map<String, Object> variables) {
    var request = ctx.request();
    var functionParams = operation.getFunction().getParameters();

    if (functionParams == null || functionParams.getProperties() == null) {
      return;
    }

    var properties = functionParams.getProperties();
    var queryParams = request.params();
    var pathParams = ctx.pathParams();

    // Merge query and path parameters, giving precedence to path params
    Map<String, String> combinedParams = new HashMap<>();
    for (String key : queryParams.names()) {
      combinedParams.put(key, queryParams.get(key));
    }
    combinedParams.putAll(pathParams); // path params take precedence

    for (Map.Entry<String, FunctionDefinition.Argument> entry : properties.entrySet()) {
      var paramName = entry.getKey();
      if (combinedParams.containsKey(paramName)) {
        var value = combinedParams.get(paramName);
        var convertedValue = convertParameterValue(value, entry.getValue());
        variables.put(paramName, convertedValue);
      }
    }
  }

  protected static void extractPostParameters(RoutingContext ctx, Map<String, Object> variables) {
    JsonObject body = ctx.body().asJsonObject();
    if (body != null) {
      variables.putAll(body.getMap());
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
  public static class ValidationException extends Exception {
    public ValidationException(String message) {
      super(message);
    }
  }
}
