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

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ScriptApiConfigImplTest {

  private SqrlConfig config;

  @BeforeEach
  void setUp() {
    config = SqrlConfig.createCurrentVersion();
  }

  @Test
  void schemaOnly() {
    var v1 = config.getSubConfig("v1");
    v1.setProperty("schema", "my-schema");

    var scriptApiConfig = new ScriptApiConfigImpl(v1);

    assertThat(scriptApiConfig.getVersion()).isEqualTo("v1");
    assertThat(scriptApiConfig.getSchema()).isEqualTo("my-schema");
    assertThat(scriptApiConfig.getOperations()).isEmpty();
  }

  @Test
  void schemaAndOneOperation() {
    var v1 = config.getSubConfig("v1");
    v1.setProperty("schema", "my-schema");
    v1.setProperty("operations", List.of("op"));

    var scriptApiConfig = new ScriptApiConfigImpl(v1);

    assertThat(scriptApiConfig.getSchema()).isEqualTo("my-schema");
    assertThat(scriptApiConfig.getOperations()).containsExactly("op");
  }

  @Test
  void schemaAndMultipleOperations() {
    var v1 = config.getSubConfig("v1");
    v1.setProperty("schema", "my-schema");
    v1.setProperty("operations", List.of("op1", "op2"));

    var scriptApiConfig = new ScriptApiConfigImpl(v1);

    assertThat(scriptApiConfig.getSchema()).isEqualTo("my-schema");
    assertThat(scriptApiConfig.getOperations()).containsExactly("op1", "op2");
  }
}
