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
import com.datasqrl.server.config.ServerConfigUtil;
import com.datasqrl.server.graphql.ModelContainer;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.modules.ApiDeploymentModule;
import com.datasqrl.server.modules.GlobalHandlersModule;
import com.datasqrl.server.modules.HealthChecksModule;
import com.datasqrl.server.modules.MetricsModule;
import com.datasqrl.server.modules.OAuthDiscoveryModule;
import com.datasqrl.server.modules.ServerModule;
import com.datasqrl.server.modules.VertxServerModuleContext;
import com.datasqrl.util.JsonUtils;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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

  /** SQRL plan dir */
  private final Path planDir;

  // Configuration is loaded once and shared with child verticles
  /** Server configuration */
  private ServerConfig config;

  /** Server model */
  private Map<String, RootGraphQLModel> models;

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  @SuppressWarnings("unused")
  public HttpServerVerticle() {
    this(Path.of("."), null, null);
  }

  public HttpServerVerticle(
      Path planDir, ServerConfig config, Map<String, RootGraphQLModel> models) {
    this.planDir = planDir;
    this.config = config;
    this.models = models;
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

  private void bootstrap(Promise<Void> startPromise) {
    var router = Router.router(vertx);
    var moduleContext =
        new VertxServerModuleContext(vertx, router, config, models, planDir, closeables);

    configureModules(moduleContext)
        .compose(ignored -> startHttpServer(router))
        .onSuccess(
            server -> {
              log.info("HTTP server listening on port {}", server.actualPort());
              startPromise.complete();
            })
        .onFailure(
            err -> {
              log.error("Failed to start application", err);
              startPromise.fail(err);
            });
  }

  private Future<Void> configureModules(VertxServerModuleContext ctx) {
    return configureInfrastructure(ctx)
        .compose(ignored -> configureGlobalHandlers(ctx))
        .compose(ignored -> configureEndpoints(ctx))
        .compose(ignored -> deployApiVerticles(ctx));
  }

  private Future<Void> configureInfrastructure(VertxServerModuleContext ctx) {
    return configurePhase(ctx, new MetricsModule());
  }

  private Future<Void> configureGlobalHandlers(VertxServerModuleContext ctx) {
    return configurePhase(ctx, new GlobalHandlersModule());
  }

  private Future<Void> configureEndpoints(VertxServerModuleContext ctx) {
    return configurePhase(ctx, new HealthChecksModule(), new OAuthDiscoveryModule());
  }

  private Future<Void> deployApiVerticles(VertxServerModuleContext ctx) {
    return configurePhase(ctx, new ApiDeploymentModule());
  }

  @SafeVarargs
  private Future<Void> configurePhase(
      VertxServerModuleContext ctx, ServerModule<VertxServerModuleContext>... modules) {

    CompletionStage<Void> chain = CompletableFuture.completedStage(null);

    for (var module : modules) {
      chain = chain.thenCompose(ignored -> module.configure(ctx));
    }

    return Future.fromCompletionStage(chain);
  }

  private Future<HttpServer> startHttpServer(Router router) {
    return vertx
        .createHttpServer(config.getHttpServerOptions())
        .requestHandler(router)
        .listen(config.getHttpServerOptions().getPort());
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
                          JsonUtils.MAPPER.readValue(result.result().toString(), Map.class));
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
  public static Map<String, RootGraphQLModel> loadModel() {
    return JsonUtils.MAPPER.readValue(
            new File("vertx.json").getAbsoluteFile(), ModelContainer.class)
        .models;
  }
}
