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

class EnginesConfigImplTest {

  private SqrlConfig config;

  @BeforeEach
  void setUp() {
    config = SqrlConfig.createCurrentVersion();
  }

  @Test
  void givenConfigWithEngines_whenGetEngineConfig_thenReturnsEngineConfig() {
    config.getSubConfig("flink").setProperty("type", "flink");
    config.getSubConfig("postgres").setProperty("type", "postgres");

    var enginesConfig = new EnginesConfigImpl(config);

    assertThat(enginesConfig.getEngineConfig("flink")).isPresent();
    assertThat(enginesConfig.getEngineConfig("postgres")).isPresent();
    assertThat(enginesConfig.getEngineConfig("nonexistent")).isEmpty();
  }

  @Test
  void givenEnginesConfig_whenGetVersion_thenReturnsConfigVersion() {
    var enginesConfig = new EnginesConfigImpl(config);

    var version = enginesConfig.getVersion();

    assertThat(version).isEqualTo(1);
  }

  @Test
  void givenConfigWithEngine_whenGetEngineConfigOrErr_thenReturnsEngineConfig() {
    config.getSubConfig("flink").setProperty("type", "flink");

    var enginesConfig = new EnginesConfigImpl(config);

    var engineConfig = enginesConfig.getEngineConfigOrErr("flink");

    assertThat(engineConfig).isNotNull();
    assertThat(engineConfig.getEngineName()).isEqualTo("flink");
  }
}
