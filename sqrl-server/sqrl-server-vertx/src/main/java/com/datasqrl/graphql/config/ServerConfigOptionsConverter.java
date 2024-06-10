package com.datasqrl.graphql.config;

import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.handler.graphql.ApolloWSOptions;
import io.vertx.ext.web.handler.graphql.GraphQLHandlerOptions;
import io.vertx.ext.web.handler.graphql.GraphiQLHandlerOptions;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.PoolOptions;

public class ServerConfigOptionsConverter {

  public static void fromJson(JsonObject json, ServerConfig serverConfig) {
    serverConfig.setServletConfig(
        new ServletConfig(json.getJsonObject("servletConfig") == null
            ? new JsonObject() : json.getJsonObject("servletConfig")));
    serverConfig.setGraphQLHandlerOptions(
        new GraphQLHandlerOptions(json.getJsonObject("graphQLHandlerOptions") == null
            ? new JsonObject() : json.getJsonObject("graphQLHandlerOptions")));
    if (json.containsKey("graphiQLHandlerOptions")) {
      serverConfig.setGraphiQLHandlerOptions(
          new GraphiQLHandlerOptions(json.getJsonObject("graphiQLHandlerOptions")));
    }
    serverConfig.setHttpServerOptions(
        new HttpServerOptions(json.getJsonObject("httpServerOptions") == null
            ? new JsonObject() : json.getJsonObject("httpServerOptions")));
    PgConnectOptions pgConnectOptions =  json.getJsonObject("pgConnectOptions") == null
        ? PgConnectOptions.fromEnv()
        : new PgConnectOptions(json.getJsonObject("pgConnectOptions"));
    serverConfig.setPgConnectOptions(pgConnectOptions);
    serverConfig.setPoolOptions(
        new PoolOptions(json.getJsonObject("poolOptions") == null
            ? new JsonObject() : json.getJsonObject("poolOptions")));
    serverConfig.setCorsHandlerOptions(
        new CorsHandlerOptions(json.getJsonObject("corsHandlerOptions") == null
            ? new JsonObject() : json.getJsonObject("corsHandlerOptions")));
    if (json.getJsonObject("authOptions") != null) {
      serverConfig.setAuthOptions(
          new JWTAuthOptions(json.getJsonObject("authOptions")));
    }
  }
}
