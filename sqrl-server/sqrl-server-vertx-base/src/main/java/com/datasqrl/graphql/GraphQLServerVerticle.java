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
import com.datasqrl.graphql.jdbc.DatabaseType;
import com.datasqrl.graphql.server.CustomScalars;
import com.datasqrl.graphql.server.GraphQLEngineBuilder;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.symbaloo.graphqlmicrometer.MicrometerInstrumentation;
import graphql.GraphQL;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.JWTAuthHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandler;
import io.vertx.ext.web.handler.graphql.GraphiQLHandler;
import io.vertx.ext.web.handler.graphql.ws.GraphQLWSHandler;
import io.vertx.micrometer.backends.BackendRegistries;
import io.vertx.sqlclient.SqlClient;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * GraphQL-specific verticle that configures GraphQL handlers on a shared router. This verticle is
 * instantiated by HttpServerVerticle with shared config, model, and JWT auth provider.
 */
@Slf4j
public class GraphQLServerVerticle extends AbstractVerticle {

  private final Router router;
  private final ServerConfig config;
  private final RootGraphqlModel model;
  private final Optional<JWTAuth> jwtAuth;
  private final JdbcClientsConfig jdbcClientsConfig;

  public GraphQLServerVerticle(
      Router router, ServerConfig config, RootGraphqlModel model, Optional<JWTAuth> jwtAuth) {
    this.router = router;
    this.config = config;
    this.model = model;
    this.jwtAuth = jwtAuth;
    this.jdbcClientsConfig = new JdbcClientsConfig(vertx, config);
  }

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
   * TODO: @Marvin need to clean up auth in here
   *
   * @param startPromise
   */
  protected void setupGraphQLRoutes(Promise<Void> startPromise) {
    // Setup GraphiQL handler if configured
    if (this.config.getGraphiQLHandlerOptions() != null) {
      var handlerBuilder =
          GraphiQLHandler.builder(vertx).with(this.config.getGraphiQLHandlerOptions());
      if (this.config.getAuthOptions() != null) {
        handlerBuilder.addingHeaders(
            rc -> {
              String token = rc.get("token");
              return MultiMap.caseInsensitiveMultiMap().add("Authorization", "Bearer " + token);
            });
      }

      var handler = handlerBuilder.build();
      router
          .route(this.config.getServletConfig().getGraphiQLEndpoint())
          .subRouter(handler.router());
    }

    // Setup database clients
    Map<DatabaseType, SqlClient> clients = jdbcClientsConfig.createClients();

    // Create GraphQL engine
    var graphQL = createGraphQL(clients, startPromise);

    // Setup GraphQL endpoint with auth if configured
    var handler = router.route(this.config.getServletConfig().getGraphQLEndpoint());
    jwtAuth.ifPresent(
        auth -> {
          // Required for adding auth on ws handler
          System.setProperty("io.vertx.web.router.setup.lenient", "true");
          handler.handler(JWTAuthHandler.create(auth));
        });

    handler
        .handler(GraphQLWSHandler.create(graphQL))
        .handler(GraphQLHandler.create(graphQL, this.config.getGraphQLHandlerOptions()));

    startPromise.complete();
  }

  public GraphQL createGraphQL(Map<DatabaseType, SqlClient> client, Promise<Void> startPromise) {
    try {
      var vertxJdbcClient = new VertxJdbcClient(client);
      var graphQL =
          model.accept(
              new GraphQLEngineBuilder.Builder()
                  .withMutationConfiguration(new MutationConfigurationImpl(model, vertx, config))
                  .withSubscriptionConfiguration(
                      new SubscriptionConfigurationImpl(
                          model, vertx, config, startPromise, vertxJdbcClient))
                  .withExtendedScalarTypes(List.of(CustomScalars.GRAPHQL_BIGINTEGER))
                  .build(),
              new VertxContext(vertxJdbcClient));
      var meterRegistry = BackendRegistries.getDefaultNow();
      if (meterRegistry != null) {
        graphQL.instrumentation(new MicrometerInstrumentation(meterRegistry));
      }
      return graphQL.build();
    } catch (Exception e) {
      startPromise.fail(e.getMessage());
      log.error("Unable to create GraphQL", e);
      throw e;
    }
  }
}
