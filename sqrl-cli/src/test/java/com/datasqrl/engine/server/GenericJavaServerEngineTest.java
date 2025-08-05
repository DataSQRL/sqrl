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

import static com.datasqrl.graphql.SqrlObjectMapper.MAPPER;
import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.config.PackageJsonImpl;
import com.datasqrl.graphql.config.ServerConfigUtil;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

class GenericJavaServerEngineTest {

  GenericJavaServerEngine underTest = new GenericJavaServerEngine("", new PackageJsonImpl()) {};

  @Test
  void test() {
    // innermost object: a single pub-/sec key
    Map<String, Object> pubSecKey =
        Map.of(
            "algorithm", "HS256",
            "buffer", "dGVzdFNlY3JldA==");

    // JWT options
    Map<String, Object> jwtOptions =
        Map.of(
            "issuer", "my-test-issuer",
            "audience", List.of("my-test-audience"),
            "expiresInSeconds", "3600",
            "leeway", "60");

    // jwtAuth node
    Map<String, Object> jwtAuth =
        Map.of("pubSecKeys", List.of(pubSecKey), "jwtOptions", jwtOptions);

    // root
    Map<String, Object> config = Map.of("jwtAuth", jwtAuth);

    var defaultConfig = underTest.readDefaultConfig();
    assertThat(defaultConfig.getJwtAuth()).isNull();

    var result = ServerConfigUtil.mergeConfigs(defaultConfig, config);

    assertThat(result).isNotNull();
    assertThat(result.getJwtAuth()).isNotNull();
    assertThat(result.getJwtAuth().getPubSecKeys()).isNotNull().isNotEmpty();
    assertThat(result.getJwtAuth().getJWTOptions()).isNotNull();
    assertThat(result.getJwtAuth().getJWTOptions().getIssuer()).isEqualTo("my-test-issuer");
  }

  @Test
  @SneakyThrows
  void givenJwtConfiguration_whenConfigMerged_thenBuffersAreStringValues() {
    // Create JWT configuration with buffer as simple string (not complex object)
    Map<String, Object> pubSecKey =
        Map.of(
            "algorithm", "HS256",
            "buffer", "dGVzdFNlY3JldA==");

    Map<String, Object> jwtOptions =
        Map.of(
            "issuer", "my-test-issuer",
            "audience", List.of("my-test-audience"),
            "expiresInSeconds", "3600",
            "leeway", "60");

    Map<String, Object> jwtAuth =
        Map.of("pubSecKeys", List.of(pubSecKey), "jwtOptions", jwtOptions);

    Map<String, Object> config = Map.of("jwtAuth", jwtAuth);

    // Test the configuration merging that would happen during serverConfig() generation
    var defaultConfig = underTest.readDefaultConfig();
    var mergedConfig = ServerConfigUtil.mergeConfigs(defaultConfig, config);

    // Serialize the merged configuration to JSON string and parse back
    var serializedJson = MAPPER.writeValueAsString(mergedConfig);
    JsonNode configNode = MAPPER.readTree(serializedJson);
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
}
