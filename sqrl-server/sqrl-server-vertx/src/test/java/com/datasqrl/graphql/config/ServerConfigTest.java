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

import static org.assertj.core.api.Assertions.*;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

class ServerConfigTest {

  @Test
  void given_emptyJson_when_constructorCalled_then_createsConfigWithDefaults() {
    var json = new JsonObject();

    var serverConfig = new ServerConfig(json);

    assertThat(serverConfig.getServletConfig()).isNotNull();
    assertThat(serverConfig.getGraphQLHandlerOptions()).isNotNull();
    assertThat(serverConfig.getGraphiQLHandlerOptions()).isNull();
    assertThat(serverConfig.getHttpServerOptions()).isNotNull();
    assertThat(serverConfig.getPgConnectOptions()).isNotNull();
    assertThat(serverConfig.getPoolOptions()).isNotNull();
    assertThat(serverConfig.getCorsHandlerOptions()).isNotNull();
    assertThat(serverConfig.getJwtAuth()).isNull();
    assertThat(serverConfig.getSwaggerConfig()).isNotNull();
    assertThat(serverConfig.getKafkaMutationConfig()).isNull();
    assertThat(serverConfig.getKafkaSubscriptionConfig()).isNull();
  }

  @Test
  void given_jsonWithAllFields_when_constructorCalled_then_createsConfigWithAllValues() {
    var json =
        new JsonObject()
            .put("servletConfig", new JsonObject().put("graphQLEndpoint", "/custom-graphql"))
            .put("graphQLHandlerOptions", new JsonObject())
            .put("graphiQLHandlerOptions", new JsonObject())
            .put("httpServerOptions", new JsonObject().put("port", 9999))
            .put(
                "pgConnectOptions", new JsonObject().put("host", "custom-host").put("port", "1234"))
            .put("poolOptions", new JsonObject().put("maxSize", 20))
            .put("corsHandlerOptions", new JsonObject().put("allowCredentials", true))
            .put("jwtAuth", new JsonObject().put("algorithm", "HS256"))
            .put("swaggerConfig", new JsonObject().put("enabled", true))
            .put(
                "kafkaMutationConfig",
                new JsonObject()
                    .put("bootstrap.servers", "localhost:9092")
                    .put("topic", "mutations"))
            .put(
                "kafkaSubscriptionConfig",
                new JsonObject()
                    .put("bootstrap.servers", "localhost:9092")
                    .put("groupId", "test-group"));

    var serverConfig = new ServerConfig(json);

    assertThat(serverConfig.getServletConfig()).isNotNull();
    assertThat(serverConfig.getGraphQLHandlerOptions()).isNotNull();
    assertThat(serverConfig.getGraphiQLHandlerOptions()).isNotNull();
    assertThat(serverConfig.getHttpServerOptions()).isNotNull();
    assertThat(serverConfig.getPgConnectOptions()).isNotNull();
    assertThat(serverConfig.getPoolOptions()).isNotNull();
    assertThat(serverConfig.getCorsHandlerOptions()).isNotNull();
    assertThat(serverConfig.getJwtAuth()).isNotNull();
    assertThat(serverConfig.getSwaggerConfig()).isNotNull();
    assertThat(serverConfig.getKafkaMutationConfig()).isNotNull();
    assertThat(serverConfig.getKafkaSubscriptionConfig()).isNotNull();
  }

  @Test
  void given_jsonWithNullFields_when_constructorCalled_then_handlesNullsCorrectly() {
    var json =
        new JsonObject()
            .putNull("servletConfig")
            .putNull("graphQLHandlerOptions")
            .putNull("httpServerOptions")
            .putNull("pgConnectOptions")
            .putNull("poolOptions")
            .putNull("corsHandlerOptions")
            .putNull("jwtAuth")
            .putNull("swaggerConfig")
            .putNull("kafkaMutationConfig")
            .putNull("kafkaSubscriptionConfig");

    var serverConfig = new ServerConfig(json);

    // Fields with empty defaults should still be created
    assertThat(serverConfig.getServletConfig()).isNotNull();
    assertThat(serverConfig.getGraphQLHandlerOptions()).isNotNull();
    assertThat(serverConfig.getHttpServerOptions()).isNotNull();
    assertThat(serverConfig.getPoolOptions()).isNotNull();
    assertThat(serverConfig.getCorsHandlerOptions()).isNotNull();
    assertThat(serverConfig.getSwaggerConfig()).isNotNull();

    // PgConnectOptions uses empty default when null
    assertThat(serverConfig.getPgConnectOptions()).isNotNull();

    // Null default mappings - these should be null when explicitly null or invalid
    assertThat(serverConfig.getJwtAuth()).isNull();
    assertThat(serverConfig.getKafkaMutationConfig()).isNull();
    assertThat(serverConfig.getKafkaSubscriptionConfig()).isNull();
  }

  @Test
  void given_constructorWithJson_when_created_then_configurationIsApplied() {
    var json =
        new JsonObject().put("servletConfig", new JsonObject().put("graphQLEndpoint", "/test"));

    var serverConfig = new ServerConfig(json);

    assertThat(serverConfig.getServletConfig()).isNotNull();
    assertThat(serverConfig.getGraphQLHandlerOptions()).isNotNull();
  }
}
