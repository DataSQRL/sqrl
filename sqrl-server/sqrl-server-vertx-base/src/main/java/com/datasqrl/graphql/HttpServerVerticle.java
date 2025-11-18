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
import com.datasqrl.graphql.config.ServerConfigUtil;
import com.datasqrl.graphql.exec.FlinkExecFunctionPlan;
import com.datasqrl.graphql.server.ModelContainer;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.operation.ApiOperation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.MoreCollectors;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Bootstrap verticle that owns the HTTP server, root router and all cross-cutting handlers.
 * Protocol-specific verticles (GraphQL, REST façade, etc.) are deployed and receive the shared
 * {@link Router}.
 */
@Slf4j
public class HttpServerVerticle extends AbstractVerticle {

  /** Resources to close */
  private final List<AutoCloseable> closeables = new ArrayList<>();

  // Configuration is loaded once and shared with child verticles
  /** Server configuration */
  private ServerConfig config;

  /** Server model */
  private Map<String, RootGraphqlModel> models;

  @Nullable private Path configDir;

  // Synchronization for promise completion to prevent race conditions
  private volatile boolean startPromiseCompleted = false;
  private final Object promiseLock = new Object();

  // ---------------------------------------------------------------------------
  // Lifecyle
  // ---------------------------------------------------------------------------

  @SuppressWarnings("unused")
  public HttpServerVerticle() {
    this.config = null;
    this.models = null;
    this.configDir = null;
  }

  public HttpServerVerticle(
      ServerConfig config, Map<String, RootGraphqlModel> models, @Nullable Path configDir) {
    this.config = config;
    this.models = models;
    this.configDir = configDir;
  }

  @Override
  public void start(Promise<Void> startPromise) {
    if (this.models == null) {
      try {
        this.models = loadModel();
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
                this.config = ServerConfigUtil.fromConfigMap(raw.getMap());
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

  @Override
  public void stop(Promise<Void> stopPromise) throws Exception {
    try {
      for (AutoCloseable closeable : closeables) {
        closeable.close();
      }
      stopPromise.complete();

    } catch (Exception e) {
      stopPromise.fail(e);
    }
  }

  // ---------------------------------------------------------------------------
  // Bootstrap
  // ---------------------------------------------------------------------------

  private void completePromiseSafely(Promise<Void> promise, boolean success, Throwable error) {
    synchronized (promiseLock) {
      if (!startPromiseCompleted && !promise.future().isComplete()) {
        startPromiseCompleted = true;
        if (success) {
          promise.tryComplete();
        } else {
          promise.tryFail(error);
        }
      }
    }
  }

  private void bootstrap(Promise<Void> startPromise) {
    var router = Router.router(vertx);

    // ── Metrics ───────────────────────────────────────────────────────────────
    var meterRegistry = findMeterRegistry();
    meterRegistry.ifPresent(
        registry -> {
          registerJvmMetrics(registry);

          router
              .route("/metrics")
              .handler(
                  ctx ->
                      ctx.response()
                          .putHeader("content-type", "text/plain")
                          .end(registry.scrape()));
        });

    // ── Global handlers (CORS, body, etc.) ────────────────────────────────────
    router.route().handler(toCorsHandler(config.getCorsHandlerOptions()));
    router.route().handler(BodyHandler.create());

    // Use detailed tracing if enabled, otherwise use standard logging (must be after BodyHandler)
    if (System.getenv("SQRL_DEBUG") != null) {
      router.route().handler(new DetailedRequestTracer());
    } else {
      router.route().handler(LoggerHandler.create());
    }

    // ── Health checks ────────────────────────────────────────────────────────
    router.get("/health*").handler(HealthCheckHandler.create(vertx));

    // Deploy GraphQL verticles and collect futures
    var deploymentFutures = new ArrayList<Future<String>>();
    for (var modelEntry : models.entrySet()) {
      var deploymentFuture =
          deployVersionedModel(router, modelEntry.getKey(), modelEntry.getValue());
      deploymentFutures.add(deploymentFuture);
    }

    // Wait for all GraphQL verticles to deploy, then start HTTP server
    Future.all(deploymentFutures)
        .onSuccess(
            compositeFuture -> {
              // ── Start the HTTP server ────────────────────────────────────────────────
              vertx
                  .createHttpServer(config.getHttpServerOptions())
                  .requestHandler(router)
                  .listen(config.getHttpServerOptions().getPort())
                  .onSuccess(
                      srv -> {
                        log.info("HTTP server listening on port {}", srv.actualPort());
                        completePromiseSafely(startPromise, true, null);
                      })
                  .onFailure(
                      err -> {
                        log.error("Failed to start HTTP server", err);
                        completePromiseSafely(startPromise, false, err);
                      });
            })
        .onFailure(
            err -> {
              log.error("Failed to deploy GraphQL verticle, will trigger orderly shutdown", err);
              completePromiseSafely(startPromise, false, err);
            });
  }

  private Optional<PrometheusMeterRegistry> findMeterRegistry() {
    var registry = BackendRegistries.getDefaultNow();
    if (registry == null) {
      return Optional.empty();
    }

    log.info("Found registry: {}", registry.getClass().getSimpleName());

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

  private Future<String> deployVersionedModel(
      Router router, String modelVersion, RootGraphqlModel model) {
    var childOpts = new DeploymentOptions().setInstances(1);

    // inside bootstrap() in HttpServerVerticle
    var jwtOpt =
        Optional.ofNullable(config.getJwtAuth())
            .map(
                authCfg -> {
                  log.info("JWT authentication enabled");
                  return JWTAuth.create(vertx, authCfg);
                });

    if (jwtOpt.isEmpty()) {
      log.info("JWT authentication disabled - no JWT configuration found");
    }

    var execFunctionPlan = loadExecFunctionPlan();
    if (execFunctionPlan.isPresent()) {
      log.info("Exec function plan loaded");
    }

    var graphQLVerticle =
        new GraphQLServerVerticle(router, config, modelVersion, model, jwtOpt, execFunctionPlan);
    var hasMcp = model.getOperations().stream().anyMatch(ApiOperation::isMcpEndpoint);
    var hasRest = model.getOperations().stream().anyMatch(ApiOperation::isRestEndpoint);

    return vertx
        .deployVerticle(graphQLVerticle, childOpts)
        .onSuccess(
            graphQLDeploymentId -> {
              log.info("GraphQL verticle deployed successfully: {}", graphQLDeploymentId);
              if (hasMcp) {
                // Deploy MCP bridge verticle with access to GraphQL engine
                var mcpBridgeVerticle =
                    new McpBridgeVerticle(
                        router, config, modelVersion, model, jwtOpt, graphQLVerticle);
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
                var restBridgeVerticle =
                    new RestBridgeVerticle(
                        router, config, modelVersion, model, jwtOpt, graphQLVerticle);
                vertx
                    .deployVerticle(restBridgeVerticle, childOpts)
                    .onSuccess(
                        restDeploymentId ->
                            log.info(
                                "REST bridge verticle deployed successfully: {}", restDeploymentId))
                    .onFailure(err -> log.error("Failed to deploy REST bridge verticle", err));
              }
            })
        .onFailure(
            err ->
                log.error("Failed to deploy GraphQL verticle, will trigger orderly shutdown", err));
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

  private Optional<FlinkExecFunctionPlan> loadExecFunctionPlan() {
    var parent = configDir != null ? configDir : Path.of(".");
    var planFile = parent.resolve("vertx-exec-functions.ser");

    if (!Files.exists(planFile)) {
      return Optional.empty();
    }

    return Optional.of(FlinkExecFunctionPlan.deserialize(planFile));
  }

  private void registerJvmMetrics(PrometheusMeterRegistry registry) {
    new UptimeMetrics().bindTo(registry);
    new JvmMemoryMetrics().bindTo(registry);
    new JvmThreadMetrics().bindTo(registry);

    var jvmGcMetrics = new JvmGcMetrics();
    jvmGcMetrics.bindTo(registry);
    closeables.add(jvmGcMetrics);
  }

  @SneakyThrows
  private static Map<String, RootGraphqlModel> loadModel() {
    return getObjectMapper()
        .readValue(new File("vertx.json").getAbsoluteFile(), ModelContainer.class)
        .models;
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
    var ch =
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
}
