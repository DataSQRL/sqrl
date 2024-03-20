package com.datasqrl.graphql.config;

import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
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
    if (json.containsKey("pgConnectOptions")) {
      PgConnectOptions pgConnectOptions = new PgConnectOptions(
          json.getJsonObject("pgConnectOptions") == null
              ? new JsonObject() : json.getJsonObject("pgConnectOptions"));

      serverConfig.setPgConnectOptions(pgConnectOptions);
    }
    if (json.containsKey("jdbcConnectOptions")) {
      serverConfig.setJdbcConnectOptions(
          new JDBCConnectOptions(json.getJsonObject("jdbcConnectOptions") == null
              ? new JsonObject() : json.getJsonObject("jdbcConnectOptions")));
    }
    serverConfig.setPoolOptions(
        new PoolOptions(json.getJsonObject("poolOptions") == null
            ? new JsonObject() : json.getJsonObject("poolOptions")));
    serverConfig.setCorsHandlerOptions(
        new CorsHandlerOptions(json.getJsonObject("corsHandlerOptions") == null
            ? new JsonObject() : json.getJsonObject("corsHandlerOptions")));
    serverConfig.setApolloWSOptions(
        new ApolloWSOptions(json.getJsonObject("apolloWSOptions") == null
            ? new JsonObject() : json.getJsonObject("apolloWSOptions")));
  }

  public static void toJson(ServerConfig serverConfig, JsonObject json) {
      json.put("servletConfig", serverConfig.getServletConfig().toJson());
      json.put("graphQLHandlerOptions", serverConfig.getGraphQLHandlerOptions().toJson());
      if (serverConfig.getGraphiQLHandlerOptions() != null) {
        json.put("graphiQLHandlerOptions", serverConfig.getGraphiQLHandlerOptions().toJson());
      }
      json.put("httpServerOptions", serverConfig.getHttpServerOptions().toJson());
      if (serverConfig.getPgConnectOptions() != null) {
        json.put("pgConnectOptions", serverConfig.getPgConnectOptions().toJson());
      }
      json.put("jdbcConnectOptions", serverConfig.getJdbcConnectOptions().toJson());
      json.put("poolOptions", serverConfig.getPoolOptions().toJson());
      json.put("corsHandlerOptions", serverConfig.getCorsHandlerOptions().toJson());
      json.put("apolloWSOptions", serverConfig.getApolloWSOptions().toJson());
  }
}
