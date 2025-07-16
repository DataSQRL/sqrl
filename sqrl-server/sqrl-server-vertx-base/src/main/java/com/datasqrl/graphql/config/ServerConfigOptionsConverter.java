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
package com.datasqrl.graphql.config;

import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.handler.graphql.GraphQLHandlerOptions;
import io.vertx.ext.web.handler.graphql.GraphiQLHandlerOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.PoolOptions;

public class ServerConfigOptionsConverter {

  public static void fromJson(JsonObject json, ServerConfig serverConfig) {
    serverConfig.setServletConfig(
        new ServletConfig(
            json.getJsonObject("servletConfig") == null
                ? new JsonObject()
                : json.getJsonObject("servletConfig")));
    serverConfig.setGraphQLHandlerOptions(
        new GraphQLHandlerOptions(
            json.getJsonObject("graphQLHandlerOptions") == null
                ? new JsonObject()
                : json.getJsonObject("graphQLHandlerOptions")));
    if (json.containsKey("graphiQLHandlerOptions")) {
      serverConfig.setGraphiQLHandlerOptions(
          new GraphiQLHandlerOptions(json.getJsonObject("graphiQLHandlerOptions")));
    }
    serverConfig.setHttpServerOptions(
        new HttpServerOptions(
            json.getJsonObject("httpServerOptions") == null
                ? new JsonObject()
                : json.getJsonObject("httpServerOptions")));
    var pgConnectOptions =
        json.getJsonObject("pgConnectOptions") == null
            ? PgConnectOptions.fromEnv()
            : new PgConnectOptions(json.getJsonObject("pgConnectOptions"));
    serverConfig.setPgConnectOptions(pgConnectOptions);
    serverConfig.setPoolOptions(
        new PoolOptions(
            json.getJsonObject("poolOptions") == null
                ? new JsonObject()
                : json.getJsonObject("poolOptions")));
    serverConfig.setCorsHandlerOptions(
        new CorsHandlerOptions(
            json.getJsonObject("corsHandlerOptions") == null
                ? new JsonObject()
                : json.getJsonObject("corsHandlerOptions")));
    serverConfig.setJwtAuth(
        json.getJsonObject("jwtAuth") == null
            ? null
            : new JWTAuthOptions(json.getJsonObject("jwtAuth")));
    serverConfig.setSwaggerConfig(
        new SwaggerConfig(
            json.getJsonObject("swaggerConfig") == null
                ? new JsonObject()
                : json.getJsonObject("swaggerConfig")));
  }
}
