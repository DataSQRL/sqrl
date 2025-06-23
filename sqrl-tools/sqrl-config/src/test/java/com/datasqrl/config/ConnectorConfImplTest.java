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
package com.datasqrl.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.error.ErrorCollector;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectorConfImplTest {

  private ErrorCollector errors;
  private SqrlConfig config;

  @BeforeEach
  void setUp() {
    errors = ErrorCollector.root();
    config = SqrlConfig.createCurrentVersion();
  }

  @Test
  void givenConnectorConfigWithProperties_whenToMap_thenReturnsAllProperties() {
    config.setProperty("url", "jdbc:postgresql://localhost:5432/db");
    config.setProperty("user", "testuser");
    config.setProperty("password", "testpass");

    ConnectorConfImpl connectorConf = new ConnectorConfImpl(config);

    Map<String, Object> map = connectorConf.toMap();
    assertThat(map).containsEntry("url", "jdbc:postgresql://localhost:5432/db");
    assertThat(map).containsEntry("user", "testuser");
    assertThat(map).containsEntry("password", "testpass");
  }

  @Test
  void givenConfigWithVariables_whenToMapWithSubstitution_thenSubstitutesVariables() {
    config.setProperty("url", "jdbc:postgresql://${sqrl:host}:${sqrl:port}/db");
    config.setProperty("user", "${sqrl:username}");

    ConnectorConfImpl connectorConf = new ConnectorConfImpl(config);

    Map<String, String> variables =
        Map.of(
            "host", "myhost",
            "port", "5432",
            "username", "myuser");

    Map<String, Object> map = connectorConf.toMapWithSubstitution(variables);
    assertThat(map).containsEntry("url", "jdbc:postgresql://myhost:5432/db");
    assertThat(map).containsEntry("user", "myuser");
  }

  @Test
  void givenConfigWithValidProperty_whenValidate_thenPasses() {
    config.setProperty("protocol", "https");
    ConnectorConfImpl connectorConf = new ConnectorConfImpl(config);

    connectorConf.validate("protocol", s -> s.startsWith("http"), "Protocol must start with http");
  }

  @Test
  void givenConfigWithInvalidProperty_whenValidate_thenThrows() {
    config.setProperty("protocol", "https");
    ConnectorConfImpl connectorConf = new ConnectorConfImpl(config);

    assertThatThrownBy(
            () -> connectorConf.validate("protocol", s -> s.equals("ftp"), "Protocol must be ftp"))
        .hasMessageContaining("Protocol must be ftp");
  }

  @Test
  void givenConfigWithUnresolvedVariable_whenToMapWithSubstitution_thenThrows() {
    config.setProperty("url", "jdbc:postgresql://${sqrl:invalidvar}/db");
    ConnectorConfImpl connectorConf = new ConnectorConfImpl(config);

    Map<String, String> variables = Map.of("host", "myhost");

    assertThatThrownBy(() -> connectorConf.toMapWithSubstitution(variables))
        .hasMessageContaining("invalid variable name");
  }

  @Test
  void givenEmptyConfigAndEmptyVariables_whenToMapWithSubstitution_thenReturnsSameMap() {
    ConnectorConfImpl connectorConf = new ConnectorConfImpl(config);

    Map<String, Object> originalMap = connectorConf.toMap();
    Map<String, Object> substitutedMap = connectorConf.toMapWithSubstitution(Map.of());

    assertThat(substitutedMap).isEqualTo(originalMap);
  }
}
