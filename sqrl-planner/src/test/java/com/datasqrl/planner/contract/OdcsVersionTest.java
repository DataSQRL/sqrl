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
package com.datasqrl.planner.contract;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class OdcsVersionTest {

  @Test
  void givenV3Versions_whenParse_thenReturnsTheirTypeCapabilities() {
    assertThat(OdcsVersion.parse("v3.0.2")).isEqualTo(OdcsVersion.V3_0);
    assertThat(OdcsVersion.parse("v3.1.0")).isEqualTo(OdcsVersion.V3_1);
    assertThat(OdcsVersion.parse("v3.2.0")).isEqualTo(OdcsVersion.V3_2);

    assertThat(OdcsVersion.V3_0.hasTimestamp()).isFalse();
    assertThat(OdcsVersion.V3_1.hasTimestamp()).isTrue();
    assertThat(OdcsVersion.V3_1.hasMap()).isFalse();
    assertThat(OdcsVersion.V3_2.hasMap()).isTrue();
  }

  @Test
  void givenUnsupportedVersions_whenParse_thenTellsWhatToDo() {
    assertThatThrownBy(() -> OdcsVersion.parse("v2.2.2"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("apiVersion [v2.2.2] is not supported: convert the contract to ODCS v3");
    assertThatThrownBy(() -> OdcsVersion.parse("v4.0.0"))
        .hasMessageContaining("supported versions are v3.0, v3.1 and v3.2");
    assertThatThrownBy(() -> OdcsVersion.parse(null)).hasMessageContaining("has no apiVersion");
  }
}
