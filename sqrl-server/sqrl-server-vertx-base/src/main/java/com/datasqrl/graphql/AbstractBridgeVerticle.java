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
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** Abstract Verticle that maps requests to GraphQL queries */
@Slf4j
public abstract class AbstractBridgeVerticle extends AbstractVerticle {

  protected final Router router;
  protected final ServerConfig config;
  protected final RootGraphqlModel model;
  protected final Optional<JWTAuth> jwtAuth;
  protected final ObjectMapper objectMapper;
  protected final GraphQLServerVerticle graphQLServerVerticle;

  // Reusable JSON field names
  protected static final String JSON_ERROR = "error";
  protected static final String JSON_MESSAGE = "message";

  public AbstractBridgeVerticle(
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

  protected void handleError(
      Throwable err, RoutingContext ctx, int statusCode, String errorMessage) {
    if (statusCode == 500) log.error(errorMessage, err);
    else log.info(errorMessage, err);

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
    FunctionDefinition.Parameters parameters = operation.getFunction().getParameters();
    if (parameters == null) {
      return; // No validation required
    }
    final JsonNode arguments;
    final JsonSchema schema;
    try {
      // Build a JSON Schema from the parameters definition
      String schemaText = getSchemaMapper().writeValueAsString(parameters);
      JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
      schema = factory.getSchema(schemaText);

      // Convert the collected variables to a JsonNode
      if (variables == null || variables.isEmpty()) {
        arguments = objectMapper.readTree("{}");
      } else {
        arguments = objectMapper.valueToTree(variables);
      }
    } catch (JsonProcessingException e) {
      throw new ValidationException("Could not parse parameter JSON:" + e.getMessage());
    }

    // Validate against the schema
    Set<ValidationMessage> schemaErrors = schema.validate(arguments);
    if (!schemaErrors.isEmpty()) {
      String schemaErrorsText =
          schemaErrors.stream().map(ValidationMessage::toString).collect(Collectors.joining("; "));
      log.info("Function call had schema errors: {}", schemaErrorsText);
      throw new ValidationException("Invalid Schema: " + schemaErrorsText);
    }
  }

  protected ObjectMapper getSchemaMapper() {
    return objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  protected Future<ExecutionResult> executeGraphQLAsync(
      RoutingContext ctx, ApiOperation operation, Map<String, Object> variables) {

    GraphQL graphQLEngine = graphQLServerVerticle.getGraphQLEngine();

    // Build the ExecutionInput
    ExecutionInput execInput =
        ExecutionInput.newExecutionInput()
            .query(operation.getApiQuery().query())
            .operationName(operation.getApiQuery().queryName())
            .variables(variables)
            .build();

    // Kick off async execution (GraphQL Java spawns its own executor)
    return Future.fromCompletionStage(graphQLEngine.executeAsync(execInput));
  }

  protected Object getExecutionData(ExecutionResult executionResult, ApiOperation operation) {
    Object result = executionResult.getData();
    if (result instanceof Map resultMap && operation.removeNesting()) {
      if (resultMap.size() == 1) {
        result = resultMap.values().iterator().next(); // Get only element
      }
    }
    return result;
  }

  protected void extractGetParameters(
      RoutingContext ctx, ApiOperation operation, Map<String, Object> variables) {
    HttpServerRequest request = ctx.request();
    FunctionDefinition.Parameters functionParams = operation.getFunction().getParameters();

    if (functionParams == null || functionParams.getProperties() == null) {
      return;
    }

    Map<String, FunctionDefinition.Argument> properties = functionParams.getProperties();
    MultiMap queryParams = request.params();
    Map<String, String> pathParams = ctx.pathParams();

    // Merge query and path parameters, giving precedence to path params
    Map<String, String> combinedParams = new HashMap<>();
    for (String key : queryParams.names()) {
      combinedParams.put(key, queryParams.get(key));
    }
    combinedParams.putAll(pathParams); // path params take precedence

    for (Map.Entry<String, FunctionDefinition.Argument> entry : properties.entrySet()) {
      String paramName = entry.getKey();
      if (combinedParams.containsKey(paramName)) {
        String value = combinedParams.get(paramName);
        Object convertedValue = convertParameterValue(value, entry.getValue());
        variables.put(paramName, convertedValue);
      }
    }
  }

  protected void extractPostParameters(RoutingContext ctx, Map<String, Object> variables) {
    JsonObject body = ctx.body().asJsonObject();
    if (body != null) {
      variables.putAll(body.getMap());
    }
  }

  protected Object convertParameterValue(String value, FunctionDefinition.Argument argumentDef) {
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
