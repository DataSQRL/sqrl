package com.datasqrl.graphql.config;

import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.handler.graphql.ApolloWSOptions;
import io.vertx.ext.web.handler.graphql.GraphQLHandlerOptions;
import io.vertx.ext.web.handler.graphql.GraphiQLHandlerOptions;
import io.vertx.ext.web.handler.graphql.ws.GraphQLWSOptions;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.impl.PgPoolOptions;
import io.vertx.sqlclient.PoolOptions;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class ServerConfig {

  public ServerConfig() {
  }

  public ServerConfig(JsonObject json) {
    ServerConfigOptionsConverter.fromJson(json, this);
  }

  ServletConfig servletConfig;
  GraphQLHandlerOptions graphQLHandlerOptions;
  @Nullable
  GraphiQLHandlerOptions graphiQLHandlerOptions;
  HttpServerOptions httpServerOptions;
  @Nullable
  PgConnectOptions pgConnectOptions;
  PoolOptions poolOptions;
  CorsHandlerOptions corsHandlerOptions;
  @Nullable
  JWTAuthOptions authOptions;

  // I moved it here as I believe it belongs to the server configuration.
  // The method itself is needed for easier mocking.
  // We can consider moving it to real server configuration later.
  public String getEnvironmentVariable(String envVar) {
    return System.getenv(envVar);
  }
}
