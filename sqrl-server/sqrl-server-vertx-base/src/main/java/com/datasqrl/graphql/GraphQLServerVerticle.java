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

import static com.datasqrl.graphql.config.ServerConfigUtil.createVersionedGraphiQLHandlerOptions;

import com.datasqrl.graphql.auth.AuthMetadataReader;
import com.datasqrl.graphql.auth.JwtFailureHandler;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.exec.FlinkExecFunctionPlan;
import com.datasqrl.graphql.exec.FlinkFunctionExecutor;
import com.datasqrl.graphql.jdbc.DatabaseType;
import com.datasqrl.graphql.jdbc.JdbcClientsConfig;
import com.datasqrl.graphql.jdbc.VertxJdbcClient;
import com.datasqrl.graphql.server.CustomScalars;
import com.datasqrl.graphql.server.FunctionExecutor;
import com.datasqrl.graphql.server.GraphQLEngineBuilder;
import com.datasqrl.graphql.server.MetadataReader;
import com.datasqrl.graphql.server.MetadataType;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.google.common.collect.ImmutableMap;
import com.symbaloo.graphqlmicrometer.MicrometerInstrumentation;
import graphql.GraphQL;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.oauth2.OAuth2Auth;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.handler.ChainAuthHandler;
import io.vertx.ext.web.handler.JWTAuthHandler;
import io.vertx.ext.web.handler.OAuth2AuthHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandler;
import io.vertx.ext.web.handler.graphql.GraphiQLHandler;
import io.vertx.ext.web.handler.graphql.ws.GraphQLWSHandler;
import io.vertx.micrometer.backends.BackendRegistries;
import io.vertx.sqlclient.SqlClient;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * GraphQL-specific verticle that configures GraphQL handlers on a shared router. This verticle is
 * instantiated by HttpServerVerticle with shared config, model, and JWT auth provider.
 */
@Slf4j
@RequiredArgsConstructor
public class GraphQLServerVerticle extends AbstractVerticle {

  private final Router router;
  private final ServerConfig config;
  private final String modelVersion;
  private final RootGraphqlModel model;
  private final List<AuthenticationProvider> authProviders;
  private final Optional<FlinkExecFunctionPlan> execFunctionPlan;

  private GraphQL graphQLEngine;

  @Override
  public void start(Promise<Void> startPromise) {
    try {
      setupGraphQLRoutes(startPromise);
    } catch (Exception e) {
      log.error("Could not setup GraphQL routes", e);
      startPromise.fail(e);
    }
  }

  /**
   * Sets up GraphQL routes including authentication handlers, GraphiQL interface, and the main
   * GraphQL endpoint with WebSocket support.
   *
   * @param startPromise the promise to complete when setup is finished
   */
  protected void setupGraphQLRoutes(Promise<Void> startPromise) {
    // Setup GraphiQL handler if configured
    if (this.config.getGraphiQLHandlerOptions() != null) {
      var versionedOptions =
          createVersionedGraphiQLHandlerOptions(modelVersion, config.getGraphiQLHandlerOptions());

      var graphiQlHandler = GraphiQLHandler.builder(vertx).with(versionedOptions).build();
      router
          .route(this.config.getServletConfig().getGraphiQLEndpoint(modelVersion))
          .subRouter(graphiQlHandler.router());
    }

    // Setup database clients
    var jdbcConfig = new JdbcClientsConfig(vertx, config);
    var dbClients = jdbcConfig.createClients();

    // Setup GraphQL endpoint with auth if configured
    var handler = router.route(this.config.getServletConfig().getGraphQLEndpoint(modelVersion));
    if (!authProviders.isEmpty()) {
      log.info(
          "Applying {} authentication provider(s) to GraphQL endpoint: {}",
          authProviders.size(),
          this.config.getServletConfig().getGraphQLEndpoint(modelVersion));
      // Required for adding auth on ws handler
      System.setProperty("io.vertx.web.router.setup.lenient", "true");
      handler.handler(createChainAuthHandler());
      handler.failureHandler(new JwtFailureHandler());
    }

    // Create subscription configuration to track async subscription setups
    var subscriptionConfig = new SubscriptionConfigurationImpl(vertx, config);

    // Create GraphQL engine
    this.graphQLEngine =
        createGraphQL(
            dbClients,
            subscriptionConfig,
            createMetadataReaders(),
            new FlinkFunctionExecutor(vertx, execFunctionPlan));

    handler
        .handler(GraphQLWSHandler.create(this.graphQLEngine))
        .handler(GraphQLHandler.create(this.graphQLEngine, this.config.getGraphQLHandlerOptions()));

    // Wait for all subscriptions to be set up before completing the promise
    subscriptionConfig
        .getAllSubscriptionsFuture()
        .onSuccess(v -> startPromise.complete())
        .onFailure(startPromise::fail);
  }

  private Map<MetadataType, MetadataReader> createMetadataReaders() {
    var readers = ImmutableMap.<MetadataType, MetadataReader>builder();
    if (!authProviders.isEmpty()) {
      log.debug("Configuring authentication metadata reader");
      readers.put(MetadataType.AUTH, new AuthMetadataReader());
    }

    return readers.build();
  }

  /**
   * Creates a chained authentication handler that accepts any of the configured auth methods. Uses
   * ChainAuthHandler.any() so a request is authenticated if ANY provider succeeds.
   */
  private AuthenticationHandler createChainAuthHandler() {
    if (authProviders.size() == 1) {
      return createAuthHandler(authProviders.get(0));
    }

    var chain = ChainAuthHandler.any();
    for (var provider : authProviders) {
      chain.add(createAuthHandler(provider));
    }
    return chain;
  }

  private AuthenticationHandler createAuthHandler(AuthenticationProvider auth) {
    if (auth instanceof JWTAuth jwtAuth) {
      return JWTAuthHandler.create(jwtAuth);
    } else if (auth instanceof OAuth2Auth oauth2Auth) {
      return OAuth2AuthHandler.create(vertx, oauth2Auth);
    } else {
      throw new IllegalArgumentException("Unsupported auth provider type: " + auth.getClass());
    }
  }

  /**
   * Returns the GraphQL engine instance for internal use by other verticles.
   *
   * @return the GraphQL engine, or null if not yet initialized
   */
  public GraphQL getGraphQLEngine() {
    return this.graphQLEngine;
  }

  public GraphQL createGraphQL(
      Map<DatabaseType, SqlClient> clients,
      SubscriptionConfigurationImpl subscriptionConfig,
      Map<MetadataType, MetadataReader> metadataReaders,
      FunctionExecutor functionExecutor) {
    try {
      var vertxJdbcClient = new VertxJdbcClient(clients);
      var graphQL =
          model.accept(
              new GraphQLEngineBuilder.Builder()
                  .withMutationConfiguration(new MutationConfigurationImpl(vertx, config))
                  .withSubscriptionConfiguration(subscriptionConfig)
                  .withExtendedScalarTypes(CustomScalars.getExtendedScalars())
                  .build(),
              new VertxContext(vertxJdbcClient, metadataReaders, functionExecutor));
      var meterRegistry = BackendRegistries.getDefaultNow();
      if (meterRegistry != null) {
        graphQL.instrumentation(new MicrometerInstrumentation(meterRegistry));
      }
      return graphQL.build();
    } catch (Exception e) {
      log.error("Unable to create GraphQL", e);
      throw e;
    }
  }
}
