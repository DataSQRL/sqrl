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

import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GenericJavaServerEngineTest {

  private ObjectMapper objectMapper = new ObjectMapper();
  GenericJavaServerEngine underTest =
      new GenericJavaServerEngine("", new EmptyEngineConfig(""), objectMapper) {};

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

    var result = underTest.mergeConfigs(defaultConfig, config);

    assertThat(result).isNotNull();
    assertThat(result.getJwtAuth()).isNotNull();
    assertThat(result.getJwtAuth().getPubSecKeys()).isNotNull().isNotEmpty();
    assertThat(result.getJwtAuth().getJWTOptions()).isNotNull();
    assertThat(result.getJwtAuth().getJWTOptions().getIssuer()).isEqualTo("my-test-issuer");
  }
}
