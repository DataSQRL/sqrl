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

class CompilerConfigImplTest {

  private SqrlConfig config;

  @BeforeEach
  void setUp() {
    config = SqrlConfig.createCurrentVersion();
  }

  @Test
  void givenEmptyConfig_whenGetDefaults_thenReturnsDefaultValues() {
    CompilerConfigImpl compilerConfig = new CompilerConfigImpl(config);

    assertThat(compilerConfig.isAddArguments()).isTrue();
    assertThat(compilerConfig.isExtendedScalarTypes()).isTrue();
    assertThat(compilerConfig.getLogger()).isEqualTo("print");
    assertThat(compilerConfig.compilePlan()).isTrue();
    assertThat(compilerConfig.getSnapshotPath()).isEmpty();
  }

  @Test
  void givenCustomConfig_whenGetValues_thenReturnsCustomValues() {
    config.setProperty("addArguments", false);
    config.setProperty("extendedScalarTypes", false);
    config.setProperty("logger", "custom");
    config.setProperty("compilePlan", false);
    config.setProperty("snapshotPath", "/path/to/snapshots");

    CompilerConfigImpl compilerConfig = new CompilerConfigImpl(config);

    assertThat(compilerConfig.isAddArguments()).isFalse();
    assertThat(compilerConfig.isExtendedScalarTypes()).isFalse();
    assertThat(compilerConfig.getLogger()).isEqualTo("custom");
    assertThat(compilerConfig.compilePlan()).isFalse();
    assertThat(compilerConfig.getSnapshotPath()).contains("/path/to/snapshots");
  }

  @Test
  void givenEmptyConfig_whenSetSnapshotPath_thenUpdatesPath() {
    CompilerConfigImpl compilerConfig = new CompilerConfigImpl(config);

    compilerConfig.setSnapshotPath("/new/path");

    assertThat(compilerConfig.getSnapshotPath()).contains("/new/path");
  }

  @Test
  void givenConfig_whenGetExplain_thenReturnsExplainConfig() {
    CompilerConfigImpl compilerConfig = new CompilerConfigImpl(config);

    ExplainConfigImpl explainConfig = compilerConfig.getExplain();

    assertThat(explainConfig).isNotNull();
  }

  @Test
  void givenConfig_whenGetOutput_thenReturnsOutputConfig() {
    CompilerConfigImpl compilerConfig = new CompilerConfigImpl(config);

    OutputConfigImpl outputConfig = (OutputConfigImpl) compilerConfig.getOutput();

    assertThat(outputConfig).isNotNull();
  }
}
