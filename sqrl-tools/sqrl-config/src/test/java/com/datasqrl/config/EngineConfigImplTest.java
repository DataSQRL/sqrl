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

import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EngineConfigImplTest {

  private SqrlConfig config;

  @BeforeEach
  void setUp() {
    config = SqrlConfig.createCurrentVersion();
  }

  @Test
  void givenConfigWithType_whenGetEngineName_thenReturnsType() {
    config.setProperty("type", "flink");

    EngineConfigImpl engineConfig = new EngineConfigImpl(config);

    assertThat(engineConfig.getEngineName()).isEqualTo("flink");
  }

  @Test
  void givenConfigWithSettings_whenGetConfig_thenReturnsConfigMap() {
    SqrlConfig configSubConfig = config.getSubConfig("config");
    configSubConfig.setProperty("version", "1.19.2");
    configSubConfig.setProperty("parallelism", 4);

    EngineConfigImpl engineConfig = new EngineConfigImpl(config);

    Map<String, Object> configMap = engineConfig.getConfig();
    assertThat(configMap).containsEntry("version", "1.19.2");
    assertThat(configMap).containsEntry("parallelism", 4);
  }

  @Test
  void givenConfigWithCustomSetting_whenGetSetting_thenReturnsValue() {
    config.setProperty("customSetting", "customValue");

    EngineConfigImpl engineConfig = new EngineConfigImpl(config);

    String value = engineConfig.getSetting("customSetting", Optional.empty());
    assertThat(value).isEqualTo("customValue");
  }

  @Test
  void givenConfigWithoutSetting_whenGetSettingWithDefault_thenReturnsDefault() {
    EngineConfigImpl engineConfig = new EngineConfigImpl(config);

    String value = engineConfig.getSetting("missingSetting", Optional.of("default"));

    assertThat(value).isEqualTo("default");
  }

  @Test
  void givenConfigWithConnectors_whenGetConnectors_thenReturnsConnectorsConfig() {
    SqrlConfig connectorsSubConfig = config.getSubConfig("connectors");
    connectorsSubConfig
        .getSubConfig("jdbc")
        .setProperty("url", "jdbc:postgresql://localhost:5432/db");

    EngineConfigImpl engineConfig = new EngineConfigImpl(config);

    ConnectorsConfig connectorsConfig = engineConfig.getConnectors();

    assertThat(connectorsConfig).isNotNull();
  }
}
