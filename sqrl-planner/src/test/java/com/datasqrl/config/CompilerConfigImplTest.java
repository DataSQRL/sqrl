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

import com.datasqrl.error.CollectedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CompilerConfigImplTest {

  private SqrlConfig config;

  @BeforeEach
  void setUp() {
    config = SqrlConfig.createCurrentVersion();
  }

  @Test
  void givenCustomConfig_whenGetValues_thenReturnsCustomValues() {
    config.setProperty("extended-scalar-types", false);
    config.setProperty("logger", "custom");

    var compilerConfig = new CompilerConfigImpl(config);

    assertThat(compilerConfig.isExtendedScalarTypes()).isFalse();
    assertThat(compilerConfig.getLogger()).isEqualTo("custom");
  }

  @Test
  void givenCostModel_whenInvalid_thenThrowsError() {
    config.setProperty("cost-model", "asd");

    var compilerConfig = new CompilerConfigImpl(config);

    assertThatThrownBy(compilerConfig::getCostModel)
        .isInstanceOf(CollectedException.class)
        .hasMessage(
            "Value [asd] for key [cost-model] is not valid. Must be one of [DEFAULT, READ, WRITE]");
  }
}
