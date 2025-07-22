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

import com.datasqrl.graphql.config.CorsHandlerOptions;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.operation.ApiOperation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.MoreCollectors;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.*;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.LoggerHandler;
import io.vertx.ext.web.healthchecks.HealthCheckHandler;
import io.vertx.micrometer.backends.BackendRegistries;
import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Bootstrap verticle that owns the HTTP server, root router and all cross-cutting handlers.
 * Protocol-specific verticles (GraphQL, REST façade, etc.) are deployed and receive the shared
 * {@link Router}.
 */
@Slf4j
public class HttpServerVerticle extends AbstractVerticle {
  // Configuration is loaded once and shared with child verticles
  /** Server configuration */
  private ServerConfig config;

  /** Server model */
  private RootGraphqlModel model;

  // ---------------------------------------------------------------------------
  // Lifecyle
  // ---------------------------------------------------------------------------

  public HttpServerVerticle() {
    this.config = null;
    this.model = null;
  }

  public HttpServerVerticle(ServerConfig config, RootGraphqlModel model) {
    this.config = config;
    this.model = model;
  }

  @Override
  public void start(Promise<Void> startPromise) {
    if (this.model == null) {
      try {
        this.model = loadModel();
      } catch (Exception e) {
        startPromise.fail(e);
        return;
      }
    }

    if (this.config == null) {
      loadConfig()
          .onFailure(startPromise::fail)
          .onSuccess(
              raw -> {
                this.config = new ServerConfig(raw);
                try {
                  bootstrap(startPromise);
                } catch (Exception e) {
                  startPromise.fail(e);
                }
              });
    } else {
      try {
        bootstrap(startPromise);
      } catch (Exception e) {
        startPromise.fail(e);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Bootstrap
  // ---------------------------------------------------------------------------

  private void bootstrap(Promise<Void> startPromise) {
    Router root = Router.router(vertx);

    // ── Metrics ───────────────────────────────────────────────────────────────
    var meterRegistry = findMeterRegistry();
    meterRegistry.ifPresent(
        registry ->
            root.route("/metrics")
                .handler(
                    ctx -> {
                      ctx.response().putHeader("content-type", "text/plain").end(registry.scrape());
                    }));

    // ── Global handlers (CORS, body, etc.) ────────────────────────────────────
    root.route().handler(toCorsHandler(config.getCorsHandlerOptions()));
    root.route().handler(BodyHandler.create());

    // Use detailed tracing if enabled, otherwise use standard logging (must be after BodyHandler)
    if (Boolean.parseBoolean(System.getenv().getOrDefault("DATASQRL_TRACE_REQUESTS", "false"))) {
      root.route().handler(DetailedRequestTracer.create());
    } else {
      root.route().handler(LoggerHandler.create());
    }

    // ── Health checks ────────────────────────────────────────────────────────
    root.get("/health*").handler(HealthCheckHandler.create(vertx));

    // ── Deploy protocol-specific verticles ───────────────────────────────────
    DeploymentOptions childOpts = new DeploymentOptions().setInstances(1);

    // inside bootstrap() in HttpServerVerticle
    var jwtOpt =
        Optional.ofNullable(config.getJwtAuth()).map(authCfg -> JWTAuth.create(vertx, authCfg));

    // Deploy GraphQL verticle first
    GraphQLServerVerticle graphQLVerticle = new GraphQLServerVerticle(root, config, model, jwtOpt);
    boolean hasMcp = model.getOperations().stream().anyMatch(ApiOperation::isMcpEndpoint);
    boolean hasRest = model.getOperations().stream().anyMatch(ApiOperation::isRestEndpoint);
    vertx
        .deployVerticle(graphQLVerticle, childOpts)
        .onSuccess(
            graphQLDeploymentId -> {
              log.info("GraphQL verticle deployed successfully: {}", graphQLDeploymentId);
              if (hasMcp) {
                // Deploy MCP bridge verticle with access to GraphQL engine
                McpBridgeVerticle mcpBridgeVerticle =
                    new McpBridgeVerticle(root, config, model, jwtOpt, graphQLVerticle);
                vertx
                    .deployVerticle(mcpBridgeVerticle, childOpts)
                    .onSuccess(
                        mcpDeploymentId ->
                            log.info(
                                "MCP bridge verticle deployed successfully: {}", mcpDeploymentId))
                    .onFailure(err -> log.error("Failed to deploy MCP bridge verticle", err));
              }
              if (hasRest) {
                // Deploy REST bridge verticle with access to GraphQL engine
                RestBridgeVerticle restBridgeVerticle =
                    new RestBridgeVerticle(root, config, model, jwtOpt, graphQLVerticle);
                vertx
                    .deployVerticle(restBridgeVerticle, childOpts)
                    .onSuccess(
                        restDeploymentId ->
                            log.info(
                                "REST bridge verticle deployed successfully: {}", restDeploymentId))
                    .onFailure(err -> log.error("Failed to deploy REST bridge verticle", err));
              }
            })
        .onFailure(err -> log.error("Failed to deploy GraphQL verticle", err));

    // ── Start the HTTP server ────────────────────────────────────────────────
    vertx
        .createHttpServer(config.getHttpServerOptions())
        .requestHandler(root)
        .listen(config.getHttpServerOptions().getPort())
        .onSuccess(
            srv -> {
              log.info("HTTP server listening on port {}", srv.actualPort());
              startPromise.complete();
            })
        .onFailure(startPromise::fail);
  }

  private Optional<PrometheusMeterRegistry> findMeterRegistry() {
    var registry = BackendRegistries.getDefaultNow();
    log.info("Found registry: {}", registry != null ? registry.getClass().getSimpleName() : "null");

    if (registry instanceof PrometheusMeterRegistry meterRegistry) {
      return Optional.of(meterRegistry);
    }

    if (registry instanceof CompositeMeterRegistry compositeRegistry) {
      return compositeRegistry.getRegistries().stream()
          .filter(PrometheusMeterRegistry.class::isInstance)
          .map(PrometheusMeterRegistry.class::cast)
          .collect(MoreCollectors.toOptional());
    }

    throw new IllegalStateException(
        "Unable to register metrics to: " + registry.getClass().getSimpleName());
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Async-read <code>server-config.json</code> and return as {@link JsonObject}. */
  private Future<JsonObject> loadConfig() {
    Promise<JsonObject> promise = Promise.promise();
    vertx
        .fileSystem()
        .readFile("vertx-config.json")
        .onComplete(
            result -> {
              if (result.succeeded()) {
                try {
                  var config =
                      new JsonObject(
                          getObjectMapper().readValue(result.result().toString(), Map.class));
                  promise.complete(config);
                } catch (Exception e) {
                  e.printStackTrace();
                  promise.fail(e);
                }
              } else {
                promise.fail(result.cause());
              }
            });
    return promise.future();
  }

  @SneakyThrows
  private static RootGraphqlModel loadModel() {
    return getObjectMapper()
        .readValue(new File("vertx.json").getAbsoluteFile(), ModelContainer.class)
        .model;
  }

  public static ObjectMapper getObjectMapper() {
    var objectMapper = new ObjectMapper();
    var module = new SimpleModule();
    module.addDeserializer(String.class, new JsonEnvVarDeserializer());
    objectMapper.registerModule(module);
    return objectMapper;
  }

  /** Build a Vert.x {@link CorsHandler} from our own options DTO. */
  private CorsHandler toCorsHandler(CorsHandlerOptions opts) {
    CorsHandler ch =
        opts.getAllowedOrigin() != null
            ? CorsHandler.create().addOrigin(opts.getAllowedOrigin())
            : CorsHandler.create();

    if (opts.getAllowedOrigins() != null) {
      ch.addOrigins(opts.getAllowedOrigins());
    }

    return ch.allowedMethods(
            opts.getAllowedMethods().stream().map(HttpMethod::valueOf).collect(Collectors.toSet()))
        .allowedHeaders(opts.getAllowedHeaders())
        .exposedHeaders(opts.getExposedHeaders())
        .allowCredentials(opts.isAllowCredentials())
        .maxAgeSeconds(opts.getMaxAgeSeconds())
        .allowPrivateNetwork(opts.isAllowPrivateNetwork());
  }

  public static class ModelContainer {
    public RootGraphqlModel model;
  }
}
