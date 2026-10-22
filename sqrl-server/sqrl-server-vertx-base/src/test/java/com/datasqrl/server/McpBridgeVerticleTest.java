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
package com.datasqrl.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.util.JsonUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class McpBridgeVerticleTest {

  @ParameterizedTest
  @ValueSource(strings = {"2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25"})
  void givenSupportedRequestedVersion_whenNegotiating_thenEchoesRequestedVersion(String version) {
    var params = JsonUtils.MAPPER.createObjectNode().put("protocolVersion", version);

    assertThat(McpBridgeVerticle.negotiateProtocolVersion(params)).isEqualTo(version);
  }

  @Test
  void givenUnsupportedRequestedVersion_whenNegotiating_thenReturnsLatestVersion() {
    var params = JsonUtils.MAPPER.createObjectNode().put("protocolVersion", "2099-01-01");

    assertThat(McpBridgeVerticle.negotiateProtocolVersion(params))
        .isEqualTo(McpBridgeVerticle.LATEST_PROTOCOL_VERSION)
        .isEqualTo("2025-11-25");
  }

  @Test
  void givenMissingRequestedVersion_whenNegotiating_thenReturnsLatestVersion() {
    assertThat(McpBridgeVerticle.negotiateProtocolVersion(JsonUtils.MAPPER.createObjectNode()))
        .isEqualTo(McpBridgeVerticle.LATEST_PROTOCOL_VERSION);
    assertThat(McpBridgeVerticle.negotiateProtocolVersion(null))
        .isEqualTo(McpBridgeVerticle.LATEST_PROTOCOL_VERSION);
  }
}
