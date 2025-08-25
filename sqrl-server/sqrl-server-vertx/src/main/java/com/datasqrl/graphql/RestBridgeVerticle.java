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

import com.datasqrl.graphql.auth.JwtFailureHandler;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.operation.ApiOperation;
import com.datasqrl.graphql.server.operation.RestMethodType;
import com.datasqrl.graphql.swagger.SwaggerService;
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

  private SwaggerService swaggerService;

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
      setupSwaggerEndpoints();
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

  private void setupSwaggerEndpoints() {
    if (config.getSwaggerConfig() == null || !config.getSwaggerConfig().isEnabled()) {
      log.info("Swagger is disabled, skipping Swagger endpoints setup");
      return;
    }

    // Initialize Swagger service
    swaggerService =
        new SwaggerService(
            config.getSwaggerConfig(), model, config.getServletConfig().getRestEndpoint());

    // Setup Swagger JSON endpoint
    var swaggerJsonEndpoint = config.getSwaggerConfig().getEndpoint();
    router
        .get(swaggerJsonEndpoint)
        .handler(
            ctx -> {
              try {
                // Extract request host and port for dynamic server URL
                var requestHost = getRequestBaseUrl(ctx);
                var swaggerJson = swaggerService.generateSwaggerJson(requestHost);
                ctx.response().putHeader("content-type", "application/json").end(swaggerJson);
              } catch (Exception e) {
                log.error("Failed to generate Swagger JSON", e);
                ctx.response()
                    .setStatusCode(500)
                    .putHeader("content-type", "application/json")
                    .end("{\"error\": \"Failed to generate Swagger documentation\"}");
              }
            });

    // Setup Swagger UI endpoint
    var swaggerUIEndpoint = config.getSwaggerConfig().getUiEndpoint();
    router
        .get(swaggerUIEndpoint)
        .handler(
            ctx -> {
              try {
                var swaggerUIHtml = swaggerService.generateSwaggerUI();
                ctx.response().putHeader("content-type", "text/html").end(swaggerUIHtml);
              } catch (Exception e) {
                log.error("Failed to generate Swagger UI", e);
                ctx.response()
                    .setStatusCode(500)
                    .putHeader("content-type", "text/html")
                    .end("<html><body><h1>Error: Failed to generate Swagger UI</h1></body></html>");
              }
            });

    log.info("Swagger endpoints setup completed:");
    log.info("  Swagger JSON: {}", swaggerJsonEndpoint);
    log.info("  Swagger UI: {}", swaggerUIEndpoint);
  }

  private void createRestEndpoint(ApiOperation operation) {
    var uriTemplate = operation.getUriTemplate();
    var httpMethod = operation.getRestMethod();

    if (uriTemplate == null || httpMethod == null) {
      log.warn(
          "Skipping REST endpoint for operation {} - missing uriTemplate or httpMethod",
          operation.getName());
      return;
    }

    // Convert URI template to Vert.x route pattern
    var routePattern = convertUriTemplateToRoute(uriTemplate);

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
    jwtAuth.ifPresent(
        auth -> {
          route.handler(JWTAuthHandler.create(auth));
          route.failureHandler(new JwtFailureHandler());
        });

    // Add the REST handler
    route.handler(
        ctx -> {
          var variables = extractParameters(ctx, operation);
          try {
            var fut = bridgeRequestToGraphQL(ctx, operation, variables);
            fut.onSuccess(
                    executionResult -> {
                      ctx.response()
                          .setStatusCode(executionResult.getErrors().isEmpty() ? 200 : 400)
                          .putHeader("content-type", "application/json");
                      if (!executionResult.getErrors().isEmpty()) {
                        var json = new JsonObject();
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
                        ctx.end(json.encode());
                      } else {
                        var result = getExecutionData(executionResult, operation);
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
    var route = QUERY_PARAMS_PATTERN.matcher(uriTemplate).replaceAll("");

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
    var variables = new HashMap<String, Object>();

    if (operation.getRestMethod() == RestMethodType.GET) {
      // For GET requests, extract parameters from URL query parameters and path parameters
      extractGetParameters(ctx, operation, variables);
    } else {
      // For POST/PUT requests, use the JSON body as variables
      extractPostParameters(ctx, variables);
    }

    return variables;
  }

  /** Extract the base URL from the current request including scheme, host, and port */
  private String getRequestBaseUrl(RoutingContext ctx) {
    var scheme = ctx.request().isSSL() ? "https" : "http";
    var host = ctx.request().getHeader("Host");

    // If host header includes port, use it as-is
    if (host != null && host.contains(":")) {
      return scheme + "://" + host;
    } else {
      // Use the actual server port from the request
      var port = ctx.request().localAddress().port();
      return scheme + "://" + (host != null ? host : "localhost") + ":" + port;
    }
  }
}
