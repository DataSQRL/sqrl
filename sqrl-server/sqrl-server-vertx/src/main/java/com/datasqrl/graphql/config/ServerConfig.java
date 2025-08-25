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

import static com.datasqrl.graphql.config.ServerConfigUtil.mapFieldWithEmptyDefault;
import static com.datasqrl.graphql.config.ServerConfigUtil.mapFieldWithNullDefault;

import com.datasqrl.env.EnvVariableNames;
import com.datasqrl.env.GlobalEnvironmentStore;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.handler.graphql.GraphQLHandlerOptions;
import io.vertx.ext.web.handler.graphql.GraphiQLHandlerOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.PoolOptions;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class ServerConfig {

  ServletConfig servletConfig;
  GraphQLHandlerOptions graphQLHandlerOptions;
  GraphiQLHandlerOptions graphiQLHandlerOptions;
  HttpServerOptions httpServerOptions;
  PgConnectOptions pgConnectOptions;
  PoolOptions poolOptions;
  CorsHandlerOptions corsHandlerOptions;
  JWTAuthOptions jwtAuth;
  SwaggerConfig swaggerConfig;
  KafkaConfig.KafkaMutationConfig kafkaMutationConfig;
  KafkaConfig.KafkaSubscriptionConfig kafkaSubscriptionConfig;
  JdbcConfig duckDbConfig;
  JdbcConfig snowflakeConfig;

  public ServerConfig(JsonObject json) {
    // Empty default mappings
    servletConfig = mapFieldWithEmptyDefault(json, "servletConfig", ServletConfig::new);
    graphQLHandlerOptions =
        mapFieldWithEmptyDefault(json, "graphQLHandlerOptions", GraphQLHandlerOptions::new);
    httpServerOptions = mapFieldWithEmptyDefault(json, "httpServerOptions", HttpServerOptions::new);
    pgConnectOptions = loadPgConnectOptions(json);
    poolOptions = mapFieldWithEmptyDefault(json, "poolOptions", PoolOptions::new);
    corsHandlerOptions =
        mapFieldWithEmptyDefault(json, "corsHandlerOptions", CorsHandlerOptions::new);
    swaggerConfig = mapFieldWithEmptyDefault(json, "swaggerConfig", SwaggerConfig::new);

    // Null default mappings
    graphiQLHandlerOptions =
        mapFieldWithNullDefault(json, "graphiQLHandlerOptions", GraphiQLHandlerOptions::new);
    jwtAuth = mapFieldWithNullDefault(json, "jwtAuth", JWTAuthOptions::new);
    kafkaMutationConfig =
        mapFieldWithNullDefault(json, "kafkaMutationConfig", KafkaConfig.KafkaMutationConfig::new);
    kafkaSubscriptionConfig =
        mapFieldWithNullDefault(
            json, "kafkaSubscriptionConfig", KafkaConfig.KafkaSubscriptionConfig::new);
    duckDbConfig = mapFieldWithNullDefault(json, "duckDbConfig", JdbcConfig::new);
    snowflakeConfig = mapFieldWithNullDefault(json, "snowflakeConfig", JdbcConfig::new);
  }

  private PgConnectOptions loadPgConnectOptions(JsonObject json) {
    var fieldName = "pgConnectOptions";
    var pgConnectOptions = mapFieldWithEmptyDefault(json, fieldName, PgConnectOptions::new);

    var pgPort = GlobalEnvironmentStore.get(EnvVariableNames.POSTGRES_PORT);
    try {
      pgConnectOptions.setPort(Integer.parseInt(pgPort));
    } catch (NumberFormatException ignored) {
    }

    return pgConnectOptions;
  }
}
