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
import static org.assertj.core.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ServerConfigTest {

  @Test
  void given_emptyJson_when_constructorCalled_then_createsConfigWithDefaults() {
    var json = MAPPER.createObjectNode();

    var serverConfig = ServerConfigUtil.fromConfigMap(MAPPER.convertValue(json, Map.class));

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
    var json = MAPPER.createObjectNode();
    json.set("servletConfig", MAPPER.createObjectNode().put("graphQLEndpoint", "/custom-graphql"));
    json.set("graphQLHandlerOptions", MAPPER.createObjectNode());
    json.set("graphiQLHandlerOptions", MAPPER.createObjectNode());
    json.set("httpServerOptions", MAPPER.createObjectNode().put("port", 9999));
    json.set(
        "pgConnectOptions",
        MAPPER.createObjectNode().put("host", "custom-host").put("port", "1234"));
    json.set("poolOptions", MAPPER.createObjectNode().put("maxSize", 20));
    json.set("corsHandlerOptions", MAPPER.createObjectNode().put("allowCredentials", true));
    json.set("jwtAuth", MAPPER.createObjectNode().set("pubSecKeys", MAPPER.createArrayNode()));
    json.set("swaggerConfig", MAPPER.createObjectNode().put("enabled", true));

    var kafkaMutationConfig = MAPPER.createObjectNode();
    kafkaMutationConfig.put("bootstrap.servers", "localhost:9092");
    kafkaMutationConfig.put("topic", "mutations");
    json.set("kafkaMutationConfig", kafkaMutationConfig);

    var kafkaSubscriptionConfig = MAPPER.createObjectNode();
    kafkaSubscriptionConfig.put("bootstrap.servers", "localhost:9092");
    kafkaSubscriptionConfig.put("groupId", "test-group");
    json.set("kafkaSubscriptionConfig", kafkaSubscriptionConfig);

    var serverConfig = ServerConfigUtil.fromConfigMap(MAPPER.convertValue(json, Map.class));

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
    var json = MAPPER.createObjectNode();
    json.putNull("servletConfig");
    json.putNull("graphQLHandlerOptions");
    json.putNull("httpServerOptions");
    json.putNull("pgConnectOptions");
    json.putNull("poolOptions");
    json.putNull("corsHandlerOptions");
    json.putNull("jwtAuth");
    json.putNull("swaggerConfig");
    json.putNull("kafkaMutationConfig");
    json.putNull("kafkaSubscriptionConfig");

    var serverConfig = ServerConfigUtil.fromConfigMap(MAPPER.convertValue(json, Map.class));

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
    var json = MAPPER.createObjectNode();
    json.set("servletConfig", MAPPER.createObjectNode().put("graphQLEndpoint", "/test"));

    var serverConfig = ServerConfigUtil.fromConfigMap(MAPPER.convertValue(json, Map.class));

    assertThat(serverConfig.getServletConfig()).isNotNull();
    assertThat(serverConfig.getGraphQLHandlerOptions()).isNotNull();
  }

  @Test
  void given_corsHandlerOptionsWithWildcardHeaders_when_constructorCalled_then_allowsAllHeaders() {
    var json = MAPPER.createObjectNode();
    var corsOptions = MAPPER.createObjectNode();
    corsOptions.put("allowedOrigin", "*");
    corsOptions.set(
        "allowedMethods", MAPPER.createArrayNode().add("POST").add("GET").add("OPTIONS"));
    corsOptions.set("allowedHeaders", MAPPER.createArrayNode().add("*"));
    json.set("corsHandlerOptions", corsOptions);

    var serverConfig = ServerConfigUtil.fromConfigMap(MAPPER.convertValue(json, Map.class));

    assertThat(serverConfig.getCorsHandlerOptions()).isNotNull();
    assertThat(serverConfig.getCorsHandlerOptions().getAllowedOrigin()).isEqualTo("*");
    assertThat(serverConfig.getCorsHandlerOptions().getAllowedMethods())
        .containsExactlyInAnyOrder("POST", "GET", "OPTIONS");
    assertThat(serverConfig.getCorsHandlerOptions().getAllowedHeaders()).containsExactly("*");
  }

  @Test
  void given_jwtAuthWithUnknownProperty_when_parsed_then_failsWithClearError() {
    var json = MAPPER.createObjectNode();
    var jwtAuth = MAPPER.createObjectNode();
    jwtAuth.set("pubSecKeys", MAPPER.createArrayNode());
    jwtAuth.set("jwtoptions", MAPPER.createObjectNode().put("leeway", 60));
    json.set("jwtAuth", jwtAuth);

    assertThatThrownBy(() -> ServerConfigUtil.fromConfigMap(MAPPER.convertValue(json, Map.class)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("jwtoptions")
        .hasMessageContaining("jwtOptions");
  }

  @Test
  void given_jwtAuthWithAudience_when_roundTripped_then_audiencePreserved() {
    var json = MAPPER.createObjectNode();
    var jwtAuth = MAPPER.createObjectNode();
    var jwtOptions = MAPPER.createObjectNode();
    jwtOptions.put("leeway", 60);
    jwtOptions.set(
        "audience", MAPPER.createArrayNode().add("https://backend.api.cloud.sqrl.site/"));
    jwtAuth.set("jwtOptions", jwtOptions);
    json.set("jwtAuth", jwtAuth);

    var config = ServerConfigUtil.fromConfigMap(MAPPER.convertValue(json, Map.class));
    var serialized = MAPPER.convertValue(config, Map.class);
    @SuppressWarnings("unchecked")
    var roundTrippedAuth = (Map<String, Object>) serialized.get("jwtAuth");
    @SuppressWarnings("unchecked")
    var roundTrippedOpts = (Map<String, Object>) roundTrippedAuth.get("jwtOptions");

    assertThat(roundTrippedOpts.get("audience"))
        .isEqualTo(List.of("https://backend.api.cloud.sqrl.site/"));
    assertThat(roundTrippedOpts.get("leeway")).isEqualTo(60);
  }
}
