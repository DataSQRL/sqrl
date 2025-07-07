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
    var compilerConfig = new CompilerConfigImpl(config);

    assertThat(compilerConfig.isExtendedScalarTypes()).isTrue();
    assertThat(compilerConfig.getLogger()).isEqualTo("print");
  }

  @Test
  void givenCustomConfig_whenGetValues_thenReturnsCustomValues() {
    config.setProperty("extendedScalarTypes", false);
    config.setProperty("logger", "custom");

    var compilerConfig = new CompilerConfigImpl(config);

    assertThat(compilerConfig.isExtendedScalarTypes()).isFalse();
    assertThat(compilerConfig.getLogger()).isEqualTo("custom");
  }

  @Test
  void givenConfig_whenGetExplain_thenReturnsExplainConfig() {
    var compilerConfig = new CompilerConfigImpl(config);

    var explainConfig = compilerConfig.getExplain();

    assertThat(explainConfig).isNotNull();
  }
}
