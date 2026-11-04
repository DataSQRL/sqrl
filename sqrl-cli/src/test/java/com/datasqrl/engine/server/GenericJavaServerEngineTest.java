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
package com.datasqrl.engine.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.config.ServerConfigConverter;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.server.config.ServerConfigUtil;
import com.datasqrl.util.JsonUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

class GenericJavaServerEngineTest {

  GenericJavaServerEngine underTest =
      new GenericJavaServerEngine(
          "", new EmptyEngineConfig(""), List.of(new DummyConverter()), ErrorCollector.root()) {};

  @Test
  void test() {
    // innermost object: a single pub-/sec key
    var config = getConfigMap();

    var defaultConfig = underTest.readDefaultConfig();
    assertThat(defaultConfig.getJwtAuthOptions()).isNull();

    var result = ServerConfigUtil.mergeConfigs(defaultConfig, config);

    assertThat(result).isNotNull();
    assertThat(result.getJwtAuthOptions()).isNotNull();
    assertThat(result.getJwtAuthOptions().getPubSecKeys()).isNotNull().isNotEmpty();
    assertThat(result.getJwtAuthOptions().getJWTOptions()).isNotNull();
    assertThat(result.getJwtAuthOptions().getJWTOptions().getIssuer()).isEqualTo("my-test-issuer");
  }

  @Test
  @SneakyThrows
  void givenJwtConfiguration_whenConfigMerged_thenBuffersAreStringValues() {
    // Create JWT configuration with buffer as simple string (not complex object)
    var config = getConfigMap();

    // Test the configuration merging that would happen during serverConfig() generation
    var defaultConfig = underTest.readDefaultConfig();
    var mergedConfig = ServerConfigUtil.mergeConfigs(defaultConfig, config);

    // Serialize the merged configuration to JSON string and parse back
    var serializedJson = JsonUtils.MAPPER.writeValueAsString(mergedConfig);
    JsonNode configNode = JsonUtils.MAPPER.readTree(serializedJson);
    JsonNode pubSecKeysNode = configNode.path("jwtAuth").path("pubSecKeys");

    assertThat(pubSecKeysNode.isArray()).isTrue();
    assertThat(pubSecKeysNode.size()).isEqualTo(1);

    JsonNode firstKey = pubSecKeysNode.get(0);
    JsonNode bufferNode = firstKey.path("buffer");

    // Verify buffer is a simple string value, not a complex object with "bytes" field
    assertThat(bufferNode.isTextual()).isTrue();
    // Note: VertxModule may process base64 buffers, but it should still be a string, not a complex
    // object
    assertThat(bufferNode.asText()).contains("dGVzdFNlY3JldA");

    // Ensure buffer is not a complex object (would have been corrupted by old VertxModule usage)
    assertThat(bufferNode.isObject()).isFalse();
    assertThat(bufferNode.has("bytes")).isFalse();
  }

  private static Map<String, Object> getConfigMap() {
    var pubSecKey =
        Map.of(
            "algorithm", "HS256",
            "buffer", "dGVzdFNlY3JldA==");

    var jwtOptions =
        Map.of(
            "issuer",
            "my-test-issuer",
            "audience",
            List.of("my-test-audience"),
            "expiresInSeconds",
            3600,
            "leeway",
            30);

    var jwtAuth = Map.of("pubSecKeys", List.of(pubSecKey), "jwtOptions", jwtOptions);

    return Map.of("jwtAuth", jwtAuth);
  }

  @Test
  void givenAuthConfig_whenMergedWithDefaultTemplate_thenTrustedIssuersAreParsed() {
    var result =
        ServerConfigUtil.mergeConfigs(underTest.readDefaultConfig(), Map.of("auth", authBlock()));

    assertThat(result.getAuthConfig().authorizationServers()).containsExactly("https://a/");
    assertThat(result.getJwtAuth()).isNull();
  }

  @Test
  void givenAuthAndDeprecatedJwtAuth_whenMergedAtCompileTime_thenRejected() {
    var config = new HashMap<String, Object>(getConfigMap());
    config.put("auth", authBlock());
    var defaultConfig = underTest.readDefaultConfig();

    assertThatThrownBy(() -> ServerConfigUtil.mergeConfigs(defaultConfig, config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "'auth' cannot be combined with the deprecated 'jwtAuth' or 'oauthConfig'");
  }

  @Test
  void givenJwtAuthWithoutKeys_whenMergedAtCompileTime_thenRejected() {
    var defaultConfig = underTest.readDefaultConfig();
    Map<String, Object> config =
        Map.of("jwtAuth", Map.of("jwtOptions", Map.of("issuer", "my-issuer")));

    assertThatThrownBy(() -> ServerConfigUtil.mergeConfigs(defaultConfig, config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("'jwtAuth' has no key that can verify signatures");
  }

  private static Map<String, Object> authBlock() {
    return Map.of("issuers", List.of(Map.of("issuer", "https://a/", "audiences", List.of("x"))));
  }

  private static class DummyConverter implements ServerConfigConverter {

    @Override
    public void convert(ObjectNode serverConfig) {}
  }
}
