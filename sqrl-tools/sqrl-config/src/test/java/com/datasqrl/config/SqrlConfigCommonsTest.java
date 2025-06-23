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

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SqrlConfigCommonsTest {

  private ErrorCollector errors;

  @BeforeEach
  void setup() {
    errors = new ErrorCollector(ErrorPrefix.ROOT);
  }

  @Test
  void givenNoPaths_whenCreatingConfig_thenReturnsDefaults() {
    var underTest = SqrlConfigCommons.fromFilesPackageJson(errors, List.of());

    assertThat(underTest).isNotNull();
    assertThat(underTest.getVersion()).isEqualTo(1);
    assertThat(underTest.getEnabledEngines()).contains("vertx", "postgres", "kafka", "flink");
    assertThat(underTest.getTestConfig()).isPresent();
    assertThat(underTest.getEngines().getEngineConfig("flink")).isPresent();
    assertThat(underTest.getScriptConfig().getGraphql()).isEmpty();
    assertThat(underTest.getScriptConfig().getMainScript()).isEmpty();
  }

  @Test
  void givenSinglePath_whenCreatingConfig_thenOverridesDefaults() {
    var underTest =
        SqrlConfigCommons.fromFilesPackageJson(
            errors, List.of(Path.of("src/test/resources/config/test-package.json")));

    assertThat(underTest).isNotNull();
    assertThat(underTest.getVersion()).isEqualTo(1);

    // test-package.json overrides enabled engines ONLY
    assertThat(underTest.getEnabledEngines()).contains("test");

    assertThat(underTest.getTestConfig()).isPresent();
    assertThat(underTest.getEngines().getEngineConfig("flink")).isPresent();
    assertThat(underTest.getScriptConfig().getGraphql()).isEmpty();
    assertThat(underTest.getScriptConfig().getMainScript()).isEmpty();
  }
}
