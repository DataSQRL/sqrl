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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectorsConfigImplTest {

  private SqrlConfig config;

  @BeforeEach
  void setUp() {
    config = SqrlConfig.createCurrentVersion();
  }

  @Test
  void givenConfigWithConnectors_whenGetConnectorConfig_thenReturnsConnectorConfig() {
    config.getSubConfig("jdbc").setProperty("url", "jdbc:postgresql://localhost:5432/db");
    config.getSubConfig("kafka").setProperty("bootstrap.servers", "localhost:9092");

    ConnectorsConfigImpl connectorsConfig = new ConnectorsConfigImpl(config);

    assertThat(connectorsConfig.getConnectorConfig("jdbc")).isPresent();
    assertThat(connectorsConfig.getConnectorConfig("kafka")).isPresent();
    assertThat(connectorsConfig.getConnectorConfig("nonexistent")).isEmpty();
  }

  @Test
  void givenConfigWithConnector_whenGetConnectorConfigOrErr_thenReturnsConnectorConfig() {
    config.getSubConfig("jdbc").setProperty("url", "jdbc:postgresql://localhost:5432/db");

    ConnectorsConfigImpl connectorsConfig = new ConnectorsConfigImpl(config);

    ConnectorConf connectorConf = connectorsConfig.getConnectorConfigOrErr("jdbc");

    assertThat(connectorConf).isNotNull();
  }
}
