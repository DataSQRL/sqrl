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

import static com.datasqrl.graphql.SqrlObjectMapper.MAPPER;

import com.datasqrl.env.EnvVariableNames;
import com.datasqrl.env.GlobalEnvironmentStore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSetter;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.handler.graphql.GraphQLHandlerOptions;
import io.vertx.ext.web.handler.graphql.GraphiQLHandlerOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.PoolOptions;
import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServerConfig {

  private ServletConfig servletConfig = new ServletConfig();
  private GraphQLHandlerOptions graphQLHandlerOptions = new GraphQLHandlerOptions();
  private GraphiQLHandlerOptions graphiQLHandlerOptions;
  private HttpServerOptions httpServerOptions = new HttpServerOptions();
  private PgConnectOptions pgConnectOptions = new PgConnectOptions();
  private PoolOptions poolOptions = new PoolOptions();
  private CorsHandlerOptions corsHandlerOptions = new CorsHandlerOptions();
  private SwaggerConfig swaggerConfig = new SwaggerConfig();
  private JWTAuthOptions jwtAuth;

  private KafkaConfig.KafkaMutationConfig kafkaMutationConfig;
  private KafkaConfig.KafkaSubscriptionConfig kafkaSubscriptionConfig;
  private JdbcConfig duckDbConfig;
  private JdbcConfig snowflakeConfig;

  /**
   * Validates and applies post-deserialization logic to this ServerConfig instance. This method
   * should be called after any Jackson deserialization.
   *
   * @return this ServerConfig instance for method chaining
   */
  public ServerConfig validated() {
    // Apply PostgreSQL port from environment variable if set
    var pgPort = GlobalEnvironmentStore.get(EnvVariableNames.POSTGRES_PORT);
    try {
      if (pgPort != null && !pgPort.isEmpty()) {
        pgConnectOptions.setPort(Integer.parseInt(pgPort));
      }
    } catch (NumberFormatException ignored) {
      // Ignore invalid port numbers
    }

    // Validate Kafka configs if present
    if (kafkaMutationConfig != null) {
      kafkaMutationConfig.validateConfig();
    }
    if (kafkaSubscriptionConfig != null) {
      kafkaSubscriptionConfig.validateConfig();
    }

    return this;
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Custom JSON setters for Jackson deserialization of Vert.x classes
  ////////////////////////////////////////////////////////////////////////////////

  @JsonSetter("graphQLHandlerOptions")
  public void setGraphQLHandlerOptionsFromJson(Map<String, Object> options) {
    this.graphQLHandlerOptions = new GraphQLHandlerOptions(getJsonObjectOrEmpty(options));
  }

  @JsonSetter("httpServerOptions")
  public void setHttpServerOptionsFromJson(Map<String, Object> options) {
    this.httpServerOptions = new HttpServerOptions(getJsonObjectOrEmpty(options));
  }

  @JsonSetter("pgConnectOptions")
  public void setPgConnectOptionsFromJson(Map<String, Object> options) {
    this.pgConnectOptions = new PgConnectOptions(getJsonObjectOrEmpty(options));
  }

  @JsonSetter("poolOptions")
  public void setPoolOptionsFromJson(Map<String, Object> options) {
    this.poolOptions = new PoolOptions(getJsonObjectOrEmpty(options));
  }

  @JsonSetter("graphiQLHandlerOptions")
  public void setGraphiQLHandlerOptionsFromJson(Map<String, Object> options) {
    this.graphiQLHandlerOptions =
        options == null ? null : new GraphiQLHandlerOptions(new JsonObject(options));
  }

  @JsonSetter("jwtAuth")
  public void setJwtAuthFromJson(Map<String, Object> options) {
    this.jwtAuth = options == null ? null : new JWTAuthOptions(new JsonObject(options));
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Custom JSON setters for Jackson deserialization of our own POJO classes
  ////////////////////////////////////////////////////////////////////////////////

  @JsonSetter("servletConfig")
  public void setServletConfigFromJson(Map<String, Object> options) {
    this.servletConfig =
        options == null ? new ServletConfig() : MAPPER.convertValue(options, ServletConfig.class);
  }

  @JsonSetter("corsHandlerOptions")
  public void setCorsHandlerOptionsFromJson(Map<String, Object> options) {
    this.corsHandlerOptions =
        options == null
            ? new CorsHandlerOptions()
            : MAPPER.convertValue(options, CorsHandlerOptions.class);
  }

  @JsonSetter("swaggerConfig")
  public void setSwaggerConfigFromJson(Map<String, Object> options) {
    this.swaggerConfig =
        options == null ? new SwaggerConfig() : MAPPER.convertValue(options, SwaggerConfig.class);
  }

  private JsonObject getJsonObjectOrEmpty(Map<String, Object> options) {
    return options == null ? new JsonObject() : new JsonObject(options);
  }
}
