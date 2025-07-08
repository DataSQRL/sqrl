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
import com.datasqrl.graphql.server.operation.RestMethodType;
import graphql.ExecutionResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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
public class RestBridgeVerticle extends AbstractBridgeVerticle {

  public static final String RESULT_DATA_KEY = "data";

  // Pattern for RFC 6570 URI template query parameters: {?param1,param2}
  private static final Pattern QUERY_PARAMS_PATTERN = Pattern.compile("\\{\\?([^}]+)\\}");
  // Pattern for RFC 6570 URI template path parameters: {param}
  private static final Pattern PATH_PARAMS_PATTERN = Pattern.compile("\\{([^}?]+)\\}");

  public RestBridgeVerticle(
      Router router,
      ServerConfig config,
      RootGraphqlModel model,
      Optional<JWTAuth> jwtAuth,
      GraphQLServerVerticle graphQLServerVerticle) {
    super(router, config, model, jwtAuth, graphQLServerVerticle);
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
    RestMethodType httpMethod = operation.getRestMethod();

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
          case NONE -> throw new UnsupportedOperationException("Should not be called");
        };

    // Add JWT auth if configured
    jwtAuth.ifPresent(auth -> route.handler(JWTAuthHandler.create(auth)));

    // Add the REST handler
    route.handler(
        ctx -> {
          Map<String, Object> variables = extractParameters(ctx, operation);
          try {
            Future<ExecutionResult> fut = bridgeRequestToGraphQL(ctx, operation, variables);
            fut.onSuccess(
                    executionResult -> {
                      ctx.response()
                          .setStatusCode(executionResult.getErrors().isEmpty() ? 200 : 400)
                          .putHeader("content-type", "application/json");
                      if (!executionResult.getErrors().isEmpty()) {
                        JsonObject json = new JsonObject();
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
                      } else {
                        Object result = getExecutionData(executionResult, operation);
                        ctx.end(new JsonObject().put(RESULT_DATA_KEY, result).encode());
                      }
                    })
                .onFailure(err -> handleError(err, ctx, 500, "Error in query processing"));
          } catch (ValidationException e) {
            handleError(e, ctx, 400, "Parameters are invalid");
          } catch (Exception e) {
            handleError(e, ctx, 500, "Error in REST query processing");
          }
        });
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

  protected static Map<String, Object> extractParameters(
      RoutingContext ctx, ApiOperation operation) {
    Map<String, Object> variables = new HashMap<>();

    if (operation.getRestMethod() == RestMethodType.GET) {
      // For GET requests, extract parameters from URL query parameters and path parameters
      extractGetParameters(ctx, operation, variables);
    } else {
      // For POST/PUT requests, use the JSON body as variables
      extractPostParameters(ctx, variables);
    }

    return variables;
  }
}
