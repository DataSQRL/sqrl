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
import com.datasqrl.graphql.server.operation.FunctionDefinition;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.dialect.Dialects;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/** REST-to-GraphQL bridge controller. Replaces the Vert.x-based RestBridgeVerticle. */
@Slf4j
@RestController
@RequiredArgsConstructor
public class RestBridgeController {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String RESULT_DATA_KEY = "data";

  private final Map<String, GraphQL> graphQLEngines;
  private final Map<String, RootGraphqlModel> rootGraphqlModels;
  private final ServerConfigProperties config;

  @GetMapping(value = "/rest/**", produces = MediaType.APPLICATION_JSON_VALUE)
  public Mono<ResponseEntity<Map<String, Object>>> handleRestGet(
      @RequestParam Map<String, String> queryParams,
      Principal principal,
      org.springframework.web.server.ServerWebExchange exchange) {
    return handleRestRequest("", queryParams, null, principal, exchange, true);
  }

  @PostMapping(
      value = "/rest/**",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public Mono<ResponseEntity<Map<String, Object>>> handleRestPost(
      @RequestBody Map<String, Object> body,
      Principal principal,
      org.springframework.web.server.ServerWebExchange exchange) {
    return handleRestRequest("", Map.of(), body, principal, exchange, false);
  }

  @GetMapping(value = "/{version}/rest/**", produces = MediaType.APPLICATION_JSON_VALUE)
  public Mono<ResponseEntity<Map<String, Object>>> handleVersionedRestGet(
      @PathVariable String version,
      @RequestParam Map<String, String> queryParams,
      Principal principal,
      org.springframework.web.server.ServerWebExchange exchange) {
    return handleRestRequest(version, queryParams, null, principal, exchange, true);
  }

  @PostMapping(
      value = "/{version}/rest/**",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public Mono<ResponseEntity<Map<String, Object>>> handleVersionedRestPost(
      @PathVariable String version,
      @RequestBody Map<String, Object> body,
      Principal principal,
      org.springframework.web.server.ServerWebExchange exchange) {
    return handleRestRequest(version, Map.of(), body, principal, exchange, false);
  }

  private Mono<ResponseEntity<Map<String, Object>>> handleRestRequest(
      String version,
      Map<String, String> queryParams,
      Map<String, Object> body,
      Principal principal,
      org.springframework.web.server.ServerWebExchange exchange,
      boolean isGet) {

    var graphQL = graphQLEngines.get(version);
    var model = rootGraphqlModels.get(version);

    if (graphQL == null || model == null) {
      var errorResponse =
          Map.<String, Object>of("error", "REST API not found for version: " + version);
      return Mono.just(ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorResponse));
    }

    // Extract the operation path from the request URI
    var requestPath = exchange.getRequest().getPath().value();
    var restPrefix = config.getServletConfig().getRestEndpoint(version);
    var operationPath =
        requestPath.substring(requestPath.indexOf(restPrefix) + restPrefix.length());
    if (operationPath.startsWith("/")) {
      operationPath = operationPath.substring(1);
    }

    // Find matching operation
    var operation = findOperation(model, operationPath, isGet);
    if (operation == null) {
      var errorResponse = Map.<String, Object>of("error", "Operation not found: " + operationPath);
      return Mono.just(ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorResponse));
    }

    // Extract parameters
    Map<String, Object> variables = new HashMap<>();
    if (isGet) {
      extractGetParameters(queryParams, exchange, operation, variables);
    } else if (body != null) {
      variables.putAll(body);
    }

    // Validate parameters
    try {
      validateParameters(variables, operation);
    } catch (ValidationException e) {
      var errorResponse =
          Map.<String, Object>of("error", "Invalid parameters", "message", e.getMessage());
      return Mono.just(ResponseEntity.badRequest().body(errorResponse));
    }

    // Execute GraphQL
    return executeGraphQL(graphQL, operation, variables, principal)
        .map(
            executionResult -> {
              if (!executionResult.getErrors().isEmpty()) {
                var errorResponse = new HashMap<String, Object>();
                errorResponse.put(
                    "errors",
                    executionResult.getErrors().stream()
                        .map(
                            err ->
                                Map.of(
                                    "message",
                                    err.getMessage(),
                                    "path",
                                    err.getPath() != null ? err.getPath() : java.util.List.of()))
                        .toList());
                return ResponseEntity.badRequest().body(errorResponse);
              }

              var result = getExecutionData(executionResult, operation);
              var response = Map.<String, Object>of(RESULT_DATA_KEY, result);
              return ResponseEntity.ok(response);
            });
  }

  private ApiOperation findOperation(RootGraphqlModel model, String path, boolean isGet) {
    return model.getOperations().stream()
        .filter(op -> op.isRestEndpoint())
        .filter(op -> matchesPath(op.getUriTemplate(), path))
        .filter(
            op ->
                isGet
                    ? op.getRestMethod().name().equals("GET")
                    : op.getRestMethod().name().equals("POST"))
        .findFirst()
        .orElse(null);
  }

  private boolean matchesPath(String uriTemplate, String path) {
    // Remove query params pattern {?param1,param2}
    var templatePath = uriTemplate.replaceAll("\\{\\?[^}]+}", "");
    // Convert {param} to regex pattern
    var pattern = templatePath.replaceAll("\\{[^}]+}", "[^/]+");
    return path.matches(pattern);
  }

  private void extractGetParameters(
      Map<String, String> queryParams,
      org.springframework.web.server.ServerWebExchange exchange,
      ApiOperation operation,
      Map<String, Object> variables) {

    var functionParams = operation.getFunction().getParameters();
    if (functionParams == null || functionParams.getProperties() == null) {
      return;
    }

    var properties = functionParams.getProperties();
    var pathParams = exchange.getRequest().getPath().pathWithinApplication().value().split("/");

    for (var entry : properties.entrySet()) {
      var paramName = entry.getKey();
      if (queryParams.containsKey(paramName)) {
        var value = queryParams.get(paramName);
        var convertedValue = convertParameterValue(value, entry.getValue());
        variables.put(paramName, convertedValue);
      }
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
          yield value;
        }
      }
      case "number" -> {
        try {
          yield Double.parseDouble(value);
        } catch (NumberFormatException e) {
          yield value;
        }
      }
      case "boolean" -> Boolean.parseBoolean(value);
      default -> value;
    };
  }

  private void validateParameters(Map<String, Object> variables, ApiOperation operation)
      throws ValidationException {
    var parameters = operation.getFunction().getParameters();
    if (parameters == null) {
      return;
    }

    try {
      var schemaMapper =
          OBJECT_MAPPER.copy().setSerializationInclusion(JsonInclude.Include.NON_NULL);
      var schemaText = schemaMapper.writeValueAsString(parameters);
      var schemaRegistry = SchemaRegistry.withDefaultDialect(Dialects.getDraft202012());
      var schema = schemaRegistry.getSchema(schemaText);

      JsonNode arguments;
      if (variables == null || variables.isEmpty()) {
        arguments = OBJECT_MAPPER.readTree("{}");
      } else {
        arguments = OBJECT_MAPPER.valueToTree(variables);
      }

      var schemaErrors = schema.validate(arguments);
      if (!schemaErrors.isEmpty()) {
        var schemaErrorsText =
            schemaErrors.stream().map(Object::toString).collect(Collectors.joining("; "));
        throw new ValidationException("Invalid Schema: " + schemaErrorsText);
      }
    } catch (ValidationException e) {
      throw e;
    } catch (Exception e) {
      throw new ValidationException("Could not parse parameter JSON: " + e.getMessage());
    }
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

  public static class ValidationException extends Exception {
    public ValidationException(String message) {
      super(message);
    }
  }

  public static String convertUriTemplateToPath(String uriTemplate) {
    // Remove query params pattern {?param1,param2}
    return uriTemplate.replaceAll("\\{\\?[^}]+}", "");
  }

  public static void extractAndConvertQueryParams(
      Map<String, String> queryParams, ApiOperation operation, Map<String, Object> parameters) {

    var functionParams = operation.getFunction().getParameters();
    if (functionParams == null || functionParams.getProperties() == null) {
      return;
    }

    var properties = functionParams.getProperties();
    for (var entry : properties.entrySet()) {
      var paramName = entry.getKey();
      if (queryParams.containsKey(paramName)) {
        var value = queryParams.get(paramName);
        var convertedValue = convertStaticParameterValue(value, entry.getValue());
        parameters.put(paramName, convertedValue);
      }
    }
  }

  public static void extractAndConvertPathParams(
      Map<String, String> pathParams, ApiOperation operation, Map<String, Object> parameters) {

    var functionParams = operation.getFunction().getParameters();
    if (functionParams == null || functionParams.getProperties() == null) {
      return;
    }

    var properties = functionParams.getProperties();
    for (var entry : properties.entrySet()) {
      var paramName = entry.getKey();
      if (pathParams.containsKey(paramName)) {
        var value = pathParams.get(paramName);
        var convertedValue = convertStaticParameterValue(value, entry.getValue());
        parameters.put(paramName, convertedValue);
      }
    }
  }

  public static void extractBody(Map<String, Object> body, Map<String, Object> parameters) {
    if (body != null) {
      parameters.putAll(body);
    }
  }

  private static Object convertStaticParameterValue(
      String value, FunctionDefinition.Argument argumentDef) {
    if (argumentDef == null || argumentDef.getType() == null) {
      return value;
    }

    return switch (argumentDef.getType()) {
      case "integer" -> {
        try {
          yield Long.parseLong(value);
        } catch (NumberFormatException e) {
          yield value;
        }
      }
      case "number" -> {
        try {
          yield Double.parseDouble(value);
        } catch (NumberFormatException e) {
          yield value;
        }
      }
      case "boolean" -> Boolean.parseBoolean(value);
      default -> value;
    };
  }
}
