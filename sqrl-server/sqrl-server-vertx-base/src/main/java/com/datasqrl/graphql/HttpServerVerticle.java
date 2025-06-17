package com.datasqrl.graphql;

import com.datasqrl.graphql.config.CorsHandlerOptions;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.*;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.healthchecks.HealthCheckHandler;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.LoggerHandler;
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

  @Override
  public void start(Promise<Void> startPromise) {
    // Load model synchronously
    try {
      this.model = loadModel();
    } catch (Exception e) {
      startPromise.fail(e);
      return;
    }

    // Load server-config.json asynchronously (existing behaviour)
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
  }

  // ---------------------------------------------------------------------------
  // Bootstrap
  // ---------------------------------------------------------------------------

  private void bootstrap(Promise<Void> startPromise) {
    Router root = Router.router(vertx);
    root.route().handler(LoggerHandler.create());

    // ── Metrics ───────────────────────────────────────────────────────────────
    var registry = BackendRegistries.getDefaultNow();
    if (registry instanceof PrometheusMeterRegistry meterRegistry) {
      root.route("/metrics")
          .handler(
              ctx -> {
                ctx.response().putHeader("content-type", "text/plain").end(meterRegistry.scrape());
              });
    }

    // ── Global handlers (CORS, body, etc.) ────────────────────────────────────
    root.route().handler(toCorsHandler(config.getCorsHandlerOptions()));
    root.route().handler(BodyHandler.create());

    // ── Health checks ────────────────────────────────────────────────────────
    root.get("/health*").handler(HealthCheckHandler.create(vertx));

    // ── Deploy protocol-specific verticles ───────────────────────────────────
    DeploymentOptions childOpts = new DeploymentOptions().setInstances(1);

    // inside bootstrap() in HttpServerVerticle
    Optional<JWTAuth> jwtOpt =
        Optional.ofNullable(config.getAuthOptions()).map(authCfg -> JWTAuth.create(vertx, authCfg));

    // GraphQL
    vertx
        .deployVerticle(() -> new GraphQLServerVerticle(root, config, model, jwtOpt), childOpts)
        .onFailure(err -> log.error("Failed to deploy GraphQL verticle", err));

    // REST→GraphQL façade (optional, comment out if not yet implemented)
    vertx
        .deployVerticle(() -> new RestBridgeVerticle(root, config, model, jwtOpt), childOpts)
        .onFailure(err -> log.error("Failed to deploy REST bridge verticle", err));

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

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Async-read <code>server-config.json</code> and return as {@link JsonObject}. */
  private Future<JsonObject> loadConfig() {
    Promise<JsonObject> promise = Promise.promise();
    vertx
        .fileSystem()
        .readFile(
            "server-config.json",
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
    return GraphQLServer.getObjectMapper()
        .readValue(new File("vertx.json"), ModelContainer.class)
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
            ? CorsHandler.create(opts.getAllowedOrigin())
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
