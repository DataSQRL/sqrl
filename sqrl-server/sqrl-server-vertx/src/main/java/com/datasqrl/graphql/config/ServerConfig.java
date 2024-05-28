package com.datasqrl.graphql.config;

import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.handler.graphql.ApolloWSOptions;
import io.vertx.ext.web.handler.graphql.GraphQLHandlerOptions;
import io.vertx.ext.web.handler.graphql.GraphiQLHandlerOptions;
import io.vertx.pgclient.PgConnectOptions;
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
  ApolloWSOptions apolloWSOptions;
  JWTAuthOptions JWTAuthOptions;

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    ServerConfigOptionsConverter.toJson(this, json);
    return json;
  }
}
