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
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class ServerConfig {

  public ServerConfig() {}

  public ServerConfig(JsonObject json) {
    ServerConfigOptionsConverter.fromJson(json, this);
  }

  ServletConfig servletConfig;
  GraphQLHandlerOptions graphQLHandlerOptions;
  GraphiQLHandlerOptions graphiQLHandlerOptions;
  HttpServerOptions httpServerOptions;
  PgConnectOptions pgConnectOptions;
  PoolOptions poolOptions;
  CorsHandlerOptions corsHandlerOptions;
  JWTAuthOptions jwtAuth;
  SwaggerConfig swaggerConfig;

  // I moved it here as I believe it belongs to the server configuration.
  // The method itself is needed for easier mocking.
  // We can consider moving it to real server configuration later.
  public String getEnvironmentVariable(String envVar) {
    return System.getenv(envVar);
  }
}
