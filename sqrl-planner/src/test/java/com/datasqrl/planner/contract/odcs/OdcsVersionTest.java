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
package com.datasqrl.planner.contract.odcs;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class OdcsVersionTest {

  @Test
  void givenV32Versions_whenCheck_thenAccepted() {
    assertThatCode(() -> OdcsVersion.check("v3.2.0")).doesNotThrowAnyException();
    assertThatCode(() -> OdcsVersion.check(" v3.2.1 ")).doesNotThrowAnyException();
  }

  @Test
  void givenOtherVersions_whenCheck_thenTellsWhatToDo() {
    assertThatThrownBy(() -> OdcsVersion.check("v3.1.0"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("apiVersion [v3.1.0] is not supported: convert the contract to ODCS v3.2.0");
    assertThatThrownBy(() -> OdcsVersion.check("v2.2.2"))
        .hasMessage("apiVersion [v2.2.2] is not supported: convert the contract to ODCS v3.2.0");
    assertThatThrownBy(() -> OdcsVersion.check(null))
        .hasMessage("The contract has no apiVersion: set it to v3.2.0");
  }
}
